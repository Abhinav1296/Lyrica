import logging
import os
import re
import requests
from typing import Optional, Dict
from functools import lru_cache
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("metadata_extractor")

# APIs (free, no auth)
COVER_ART_API = "https://coverartarchive.org"
MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"
WIKIPEDIA_API = "https://en.wikipedia.org/api/rest_v1"
ITUNES_API = "https://itunes.apple.com/search"

UA = "Lyrica/1.0 (lyrics API)"

# Pooled session (keep-alive + retries)
_SESSION = requests.Session()
_retries = Retry(total=2, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=_retries)
_SESSION.mount("http://", _adapter)
_SESSION.mount("https://", _adapter)

# Reusable executor (don’t create a new one per request)
_META_THREADS = int(os.getenv("META_THREADS", "4"))
_EXECUTOR = ThreadPoolExecutor(max_workers=_META_THREADS)

# Best-artwork Redis cache TTL (30 days — album art never changes)
_ARTWORK_TTL_SECONDS = int(os.getenv("ARTWORK_CACHE_TTL", str(30 * 24 * 3600)))


# --------------------------------------------------------------------------- #
# Legacy per-source fetchers
# --------------------------------------------------------------------------- #
def get_musicbrainz_metadata(artist: str, song: str) -> Optional[Dict]:
    try:
        headers = {"User-Agent": UA}
        params = {
            "query": f"\"{song}\" AND artist:\"{artist}\"",
            "fmt": "json",
            "limit": 1,
            "inc": "tags+releases+artist-credits",
        }
        r = _SESSION.get(f"{MUSICBRAINZ_API}/recording", params=params, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            recs = data.get("recordings") or []
            if recs:
                return recs[0]
        return None
    except Exception as e:
        logger.debug(f"MusicBrainz error: {e}")
        return None


def get_wikipedia_summary(artist: str, song: str) -> Optional[Dict]:
    try:
        headers = {"User-Agent": UA}

        def _try(title: str):
            url = f"{WIKIPEDIA_API}/page/summary/{requests.utils.quote(title)}"
            r = _SESSION.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if "extract" in data:
                    return {
                        "description": data.get("extract", ""),
                        "thumbnail": data.get("thumbnail", {}).get("source", ""),
                        "url": data.get("content_urls", {}).get("desktop", {}).get("page", ""),
                    }
            return None

        out = _try(f"{song} (song)")
        if out:
            return out
        return _try(song)

    except Exception as e:
        logger.debug(f"Wikipedia error: {e}")
        return None


def get_itunes_metadata(artist: str, song: str) -> Optional[Dict]:
    try:
        params = {"term": f"{artist} {song}", "entity": "song", "limit": 1}
        r = _SESSION.get(ITUNES_API, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get("resultCount", 0) > 0:
                track = data["results"][0]
                art100 = track.get("artworkUrl100") or ""
                return {
                    "title": track.get("trackName", song),
                    "artist": track.get("artistName", artist),
                    "album": track.get("collectionName", ""),
                    "album_art": art100.replace("100x100bb.jpg", "1200x1200bb.jpg") if art100 else "",
                    "release_date": (track.get("releaseDate") or "")[:10],
                    "duration_ms": track.get("trackTimeMillis", 0),
                    "genre": track.get("primaryGenreName", ""),
                    "url": track.get("trackViewUrl", ""),
                }
        return None
    except Exception as e:
        logger.debug(f"iTunes error: {e}")
        return None


def get_lastfm_metadata(artist: str, song: str) -> Optional[Dict]:
    try:
        url = f"https://www.last.fm/music/{requests.utils.quote(artist)}/_/{requests.utils.quote(song)}"
        headers = {"User-Agent": UA}
        r = _SESSION.get(url, headers=headers, timeout=5)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        listeners_elem = soup.select_one('li[data-analytics-label="listener_count"] .metadata-display')
        playcount_elem = soup.select_one('li[data-analytics-label="scrobble_count"] .metadata-display')

        def _to_int(elem):
            if not elem:
                return 0
            t = elem.text.strip().replace(",", "")
            return int(t) if t.isdigit() else 0

        listeners = _to_int(listeners_elem)
        playcount = _to_int(playcount_elem)

        tags = []
        for tag in soup.select(".tags-list--global a")[:7]:
            tags.append(tag.text.strip())

        album_elem = soup.select_one(".header-metadata-title a")
        album = album_elem.text.strip() if album_elem else ""

        if listeners or playcount or tags or album:
            return {"playcount": playcount, "listeners": listeners, "tags": tags, "album": album, "url": url}
        return None

    except Exception as e:
        logger.debug(f"Last.fm scrape error: {e}")
        return None


def get_cover_art(release_id: str) -> Optional[str]:
    try:
        if not release_id:
            return None
        r = _SESSION.get(f"{COVER_ART_API}/release/{release_id}/front", timeout=5, allow_redirects=True, stream=True)
        if r.status_code == 200:
            return f"{COVER_ART_API}/release/{release_id}/front"
        return None
    except Exception as e:
        logger.debug(f"Cover Art error: {e}")
        return None


# --------------------------------------------------------------------------- #
# NEW: Best-artwork lookup chain (Spotify → iTunes → None)
# Result is memoized in-process AND persisted to Redis for 30 days.
# --------------------------------------------------------------------------- #
def _spotify_artwork(artist: str, song: str) -> Optional[str]:
    """
    Fetch highest-quality album art from Spotify via SpotifyScraper.
    Uses the same SPOTIFY_SP_DC cookie as the lyrics fetcher.
    Returns image URL (usually 640x640) or None.
    """
    sp_dc = (os.getenv("SPOTIFY_SP_DC") or "").strip()
    if not sp_dc:
        return None

    try:
        from spotify_scraper import SpotifyClient
    except ImportError:
        logger.debug("spotify_scraper not installed — cannot fetch Spotify artwork")
        return None

    # Telugu-biased queries, most specific first
    query_variants = [
        f'track:"{song}" artist:"{artist}" telugu',
        f'track:"{song}" artist:"{artist}"',
        f'{song} {artist} telugu',
        f'{song} telugu',
    ]

    try:
        with SpotifyClient(cookies={"sp_dc": sp_dc}) as c:
            for q in query_variants:
                try:
                    res = c.search(q, types=("track",), limit=3)
                except Exception:
                    continue
                if not res or not getattr(res, "tracks", None):
                    continue

                for t in res.tracks[:3]:
                    try:
                        album = getattr(t, "album", None)
                        if not album:
                            continue
                        images = getattr(album, "images", None) or []
                        if not images:
                            continue
                        # Spotify images are sorted largest-first
                        img = images[0]
                        url = getattr(img, "url", None) or (img.get("url") if isinstance(img, dict) else None)
                        if url:
                            logger.info(f"[artwork] spotify hit for '{artist} - {song}': {url}")
                            return url
                    except Exception:
                        continue
        return None
    except Exception as e:
        logger.debug(f"[artwork] spotify error: {e}")
        return None


def _itunes_artwork(artist: str, song: str) -> Optional[str]:
    """
    Fetch high-quality album art from iTunes Search API.
    Returns image URL (upgraded to 1200x1200) or None.
    """
    try:
        data = get_itunes_metadata(artist, song)
        if data and data.get("album_art"):
            logger.info(f"[artwork] itunes hit for '{artist} - {song}'")
            return data["album_art"]
        return None
    except Exception as e:
        logger.debug(f"[artwork] itunes error: {e}")
        return None


def _artwork_cache_key(artist: str, song: str) -> str:
    """Stable cache key for artwork lookups."""
    import hashlib
    payload = f"artwork:v1:{(artist or '').strip().lower()}|{(song or '').strip().lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_best_artwork(artist: str, song: str) -> Optional[str]:
    """
    Best-effort album art lookup with graceful fallback:
      1. Spotify (highest quality, ~640×640)
      2. iTunes  (good quality, upgraded to 1200×1200)
      3. None    (caller falls back to whatever JioSaavn provided)

    Result cached in Redis for 30 days via the shared cache layer.
    Never raises — always returns a URL string or None.
    """
    if not artist or not song:
        return None

    # Lazy import to avoid circular deps at module load
    try:
        from src.cache import load_from_cache, save_to_cache
    except Exception:
        load_from_cache = None
        save_to_cache = None

    cache_key = _artwork_cache_key(artist, song)

    # L1/L2/L3 cache lookup
    if load_from_cache:
        cached = load_from_cache(cache_key)
        if cached is not None:
            # Cache may store "" to mean "we tried, both failed"
            return cached or None

    url: Optional[str] = None

    # 1) Spotify
    try:
        url = _spotify_artwork(artist, song)
    except Exception as e:
        logger.debug(f"[artwork] spotify layer crashed: {e}")

    # 2) iTunes fallback
    if not url:
        try:
            url = _itunes_artwork(artist, song)
        except Exception as e:
            logger.debug(f"[artwork] itunes layer crashed: {e}")

    # Persist result (store "" for negative hits so we don't retry for 30 days)
    if save_to_cache:
        try:
            save_to_cache(cache_key, url or "")
        except Exception:
            pass

    if not url:
        logger.info(f"[artwork] no external art found for '{artist} - {song}'")

    return url or None


# --------------------------------------------------------------------------- #
# Aggregate metadata (existing behavior + best-artwork override)
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=500)
def get_song_metadata(artist: str, song: str) -> Dict:
    """
    Fetch metadata from multiple sources in parallel.
    This function is cached in-process via lru_cache.
    """
    try:
        tasks = {
            "musicbrainz": lambda: get_musicbrainz_metadata(artist, song),
            "itunes":      lambda: get_itunes_metadata(artist, song),
            "lastfm":      lambda: get_lastfm_metadata(artist, song),
            "wikipedia":   lambda: get_wikipedia_summary(artist, song),
            "best_art":    lambda: get_best_artwork(artist, song),
        }

        results: Dict[str, Optional[object]] = {k: None for k in tasks}

        futures = { _EXECUTOR.submit(fn): name for name, fn in tasks.items() }

        try:
            for fut in as_completed(futures, timeout=12):
                name = futures[fut]
                try:
                    results[name] = fut.result()
                except Exception as e:
                    logger.debug(f"Metadata source failed: {name}: {e}")
                    results[name] = None
        except FuturesTimeout:
            logger.info("Metadata overall timeout reached; returning partial metadata")

        mb_data = results.get("musicbrainz")
        itunes_data = results.get("itunes")
        lastfm_data = results.get("lastfm")
        wiki_data = results.get("wikipedia")
        best_art = results.get("best_art")  # str URL or None

        metadata = {}
        sources_used = []

        # MusicBrainz
        if mb_data:
            sources_used.append("MusicBrainz")
            releases = mb_data.get("releases") or []
            release_id = release_title = release_date = ""
            if releases:
                rel = releases[0]
                release_id = rel.get("id", "")
                release_title = rel.get("title", "")
                release_date = rel.get("date", "")

            metadata.update({
                "title": mb_data.get("title", song),
                "musicbrainz_id": mb_data.get("id", ""),
                "release_id": release_id,
                "release_title": release_title,
                "release_date": release_date,
                "duration_ms": mb_data.get("length", 0),
                "tags": [t.get("name") for t in (mb_data.get("tags") or [])[:5] if isinstance(t, dict)],
            })
            artist_credit = mb_data.get("artist-credit") or []
            if artist_credit:
                metadata["artist"] = artist_credit[0].get("artist", {}).get("name", artist)
            else:
                metadata["artist"] = artist
            metadata["album"] = release_title

            cover = get_cover_art(release_id)
            if cover:
                metadata["album_art"] = cover
                sources_used.append("Cover Art Archive")

        # iTunes
        if itunes_data:
            sources_used.append("iTunes")
            metadata["title"] = metadata.get("title") or itunes_data.get("title", song)
            metadata["artist"] = itunes_data.get("artist", artist)
            metadata["album"] = metadata.get("album") or itunes_data.get("album", "")
            metadata["release_date"] = metadata.get("release_date") or itunes_data.get("release_date", "")
            metadata["duration_ms"] = metadata.get("duration_ms") or itunes_data.get("duration_ms", 0)
            if not metadata.get("album_art"):
                metadata["album_art"] = itunes_data.get("album_art", "")
            if not metadata.get("tags") and itunes_data.get("genre"):
                metadata["tags"] = [itunes_data.get("genre")]
            metadata["itunes_url"] = itunes_data.get("url", "")

        # Last.fm
        if lastfm_data:
            sources_used.append("Last.fm")
            metadata["playcount"] = lastfm_data.get("playcount", 0)
            metadata["listeners"] = lastfm_data.get("listeners", 0)
            if not metadata.get("tags"):
                metadata["tags"] = lastfm_data.get("tags", [])
            if not metadata.get("album"):
                metadata["album"] = lastfm_data.get("album", "")
            metadata["lastfm_url"] = lastfm_data.get("url", "")

        # Wikipedia
        if wiki_data:
            sources_used.append("Wikipedia")
            metadata.update({
                "description": wiki_data.get("description", ""),
                "wiki_thumbnail": wiki_data.get("thumbnail", ""),
                "wiki_url": wiki_data.get("url", ""),
            })

        # BEST ARTWORK OVERRIDE — Spotify/iTunes wins over MusicBrainz Cover Art
        # (which is often empty for Telugu movies).
        if isinstance(best_art, str) and best_art:
            metadata["album_art"] = best_art
            if best_art not in ("", None):
                sources_used.append("BestArtwork")

        if not metadata:
            return {"success": False, "error": f"No metadata found for '{song}' by '{artist}'", "sources": []}

        listeners = metadata.get("listeners", 0) or 0
        metadata["popularity"] = min(100, max(0, int((listeners / 10000) ** 0.5 * 10))) if listeners else 0

        return {"success": True, "metadata": metadata, "sources": sources_used}

    except Exception as e:
        logger.error(f"Metadata retrieval error: {e}")
        return {"success": False, "error": str(e), "sources": []}


def format_metadata(metadata: Dict) -> Dict:
    try:
        duration_ms = metadata.get("duration_ms", 0) or 0
        duration_sec = duration_ms // 1000 if duration_ms else 0
        minutes = duration_sec // 60
        seconds = duration_sec % 60

        release_date = metadata.get("release_date", "") or ""
        release_year = release_date.split("-")[0] if release_date else ""

        return {
            "title": metadata.get("title", ""),
            "artist": metadata.get("artist", ""),
            "album": metadata.get("album", metadata.get("release_title", "")),
            "album_art": metadata.get("album_art", ""),
            "description": metadata.get("description", ""),
            "wiki_thumbnail": metadata.get("wiki_thumbnail", ""),
            "release_date": release_date,
            "release_year": int(release_year) if release_year.isdigit() else None,
            "duration": {
                "ms": duration_ms,
                "seconds": duration_sec,
                "formatted": f"{minutes}:{seconds:02d}" if duration_sec > 0 else "Unknown",
            },
            "popularity": metadata.get("popularity", 0),
            "playcount": metadata.get("playcount", 0),
            "listeners": metadata.get("listeners", 0),
            "tags": metadata.get("tags", []),
            "links": {
                "musicbrainz": f"https://musicbrainz.org/recording/{metadata.get('musicbrainz_id', '')}" if metadata.get("musicbrainz_id") else "",
                "lastfm": metadata.get("lastfm_url", ""),
                "itunes": metadata.get("itunes_url", ""),
                "wikipedia": metadata.get("wiki_url", ""),
            },
            "musicbrainz_id": metadata.get("musicbrainz_id", ""),
            "release_id": metadata.get("release_id", ""),
        }
    except Exception as e:
        logger.error(f"Metadata formatting error: {e}")
        return {}


def enhance_lyrics_with_metadata(lyrics_response: Dict, artist: str, song: str) -> Dict:
    try:
        meta = get_song_metadata(artist, song)
        if meta.get("success"):
            lyrics_response["metadata"] = format_metadata(meta["metadata"])
        else:
            lyrics_response["metadata"] = {"error": meta.get("error", "Could not fetch metadata"), "success": False}
        return lyrics_response
    except Exception as e:
        logger.error(f"Enhance lyrics error: {e}")
        lyrics_response["metadata"] = {"error": str(e), "success": False}
        return lyrics_response


def get_metadata_only(artist: str, song: str) -> Dict:
    try:
        meta = get_song_metadata(artist, song)
        if meta.get("success"):
            return {
                "status": "success",
                "metadata": format_metadata(meta["metadata"]),
                "sources": meta.get("sources", []),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        return {
            "status": "error",
            "error": meta.get("error", "Metadata fetch failed"),
            "sources": [],
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        logger.error(f"Get metadata only error: {e}")
        return {
            "status": "error",
            "error": str(e),
            "sources": [],
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
