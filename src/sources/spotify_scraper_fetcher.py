import os
import re
import asyncio
import unicodedata
from difflib import SequenceMatcher

from .base_fetcher import BaseFetcher, build_result


# ---------- helpers ----------

_TELUGU_UNICODE_RANGE = (0x0C00, 0x0C7F)  # Telugu script range

_TELUGU_HINTS = ("telugu", "tollywood")

def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def _has_telugu_script(text: str) -> bool:
    if not text:
        return False
    for ch in text:
        cp = ord(ch)
        if _TELUGU_UNICODE_RANGE[0] <= cp <= _TELUGU_UNICODE_RANGE[1]:
            return True
    return False


def _sim(a: str, b: str) -> float:
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _split_artists(s: str):
    if not s:
        return []
    s = re.sub(r"\s*(feat\.?|ft\.?|featuring|with|&|and)\s*", ",", s, flags=re.I)
    out = []
    seen = set()
    for part in re.split(r"\s*[,;/]\s*", s):
        n = _norm(part)
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _artist_overlap_score(req_artists, cand_artists) -> float:
    """
    Return best pairwise similarity between requested and candidate artists.
    0.0 if no plausible overlap.
    """
    if not req_artists or not cand_artists:
        return 0.0
    best = 0.0
    for r in req_artists:
        for c in cand_artists:
            best = max(best, _sim(r, c))
            # substring boost — helps "Sid Sriram" vs "Sid Sriram & X"
            if len(r) >= 3 and r in c:
                best = max(best, 0.95)
            if len(c) >= 3 and c in r:
                best = max(best, 0.9)
    return best


def _telugu_bias_score(track_obj) -> float:
    """
    Heuristic: how likely this track is a Telugu track.
    Signals:
      - album name contains 'telugu' / 'tollywood'
      - any artist string contains 'telugu'
      - title/album has Telugu script chars
    Returns 0.0–1.0
    """
    score = 0.0

    name = getattr(track_obj, "name", "") or ""
    album = ""
    try:
        album = (getattr(track_obj, "album", None) and getattr(track_obj.album, "name", "")) or ""
    except Exception:
        album = ""
    artists_names = []
    try:
        for a in (getattr(track_obj, "artists", None) or []):
            artists_names.append(getattr(a, "name", "") or "")
    except Exception:
        pass
    combined = " ".join([name, album, " ".join(artists_names)]).lower()

    if any(h in combined for h in _TELUGU_HINTS):
        score = max(score, 0.9)

    if _has_telugu_script(name) or _has_telugu_script(album):
        score = max(score, 1.0)

    return score


def _to_timed(lines):
    timed = []
    for i, ln in enumerate(lines or []):
        try:
            start_ms = int(getattr(ln, "start_ms", 0) or 0)
        except Exception:
            start_ms = 0
        text = (getattr(ln, "text", "") or "").strip()
        if not text:
            continue
        timed.append({
            "text": text,
            "start_time": start_ms,
            "end_time": None,
            "id": f"sp_{i}",
        })

    for i, e in enumerate(timed):
        if i + 1 < len(timed):
            e["end_time"] = timed[i + 1]["start_time"]
        else:
            e["end_time"] = e["start_time"] + 4000

    return timed


class SpotifyScraperFetcher(BaseFetcher):
    """
    Primary synced lyrics source using SpotifyScraper.
    Requires env var: SPOTIFY_SP_DC
    Telugu-biased search + strict artist gate to avoid Hindi/other-language matches.
    """
    source_name = "spotify_scraper"

    # thresholds (tunable via env)
    _MIN_TITLE_SIM = float(os.getenv("SP_SC_MIN_TITLE_SIM", "0.72"))
    _MIN_ARTIST_SIM = float(os.getenv("SP_SC_MIN_ARTIST_SIM", "0.72"))
    _MIN_TELUGU_BIAS = float(os.getenv("SP_SC_MIN_TELUGU_BIAS", "0.6"))

    async def fetch(self, artist: str, song: str, timestamps: bool = False):
        sp_dc = (os.getenv("SPOTIFY_SP_DC") or "").strip()
        if not sp_dc:
            return None

        loop = asyncio.get_event_loop()

        req_artists = _split_artists(artist)
        req_title_norm = _norm(song)

        def _work():
            from spotify_scraper import SpotifyClient

            # Build multiple query variants, from most specific to broadest
            query_variants = [
                f'track:"{song}" artist:"{artist}" telugu',
                f'track:"{song}" telugu',
                f'{song} {artist} telugu',
                f'{song} telugu',
                f'{song} {artist}',  # last resort
            ]

            best = None
            best_score = -1.0
            best_reason = ""

            with SpotifyClient(cookies={"sp_dc": sp_dc}) as c:
                for q in query_variants:
                    try:
                        res = c.search(q, types=("track",), limit=5)
                    except Exception:
                        continue

                    if not res or not getattr(res, "tracks", None):
                        continue

                    for t in res.tracks[:5]:
                        try:
                            t_name = getattr(t, "name", "") or ""
                            t_artists = [getattr(a, "name", "") or "" for a in (getattr(t, "artists", None) or [])]
                        except Exception:
                            continue

                        title_sim = _sim(req_title_norm, t_name)
                        artist_sim = _artist_overlap_score(req_artists, [_norm(x) for x in t_artists])
                        telugu_bias = _telugu_bias_score(t)

                        # STRICT gates
                        if title_sim < self._MIN_TITLE_SIM:
                            continue
                        if artist_sim < self._MIN_ARTIST_SIM and telugu_bias < self._MIN_TELUGU_BIAS:
                            # need at least one of (strong artist match) or (strong Telugu signal)
                            continue

                        # Composite score:
                        # 0.55 title + 0.30 artist + 0.15 telugu bias
                        score = (title_sim * 0.55) + (artist_sim * 0.30) + (telugu_bias * 0.15)

                        if score > best_score:
                            best_score = score
                            best = t
                            best_reason = (
                                f"query='{q}' title_sim={title_sim:.2f} "
                                f"artist_sim={artist_sim:.2f} telugu={telugu_bias:.2f}"
                            )

                    # If we found something reasonable on a stronger-biased query, stop early
                    if best is not None and "telugu" in q.lower():
                        break

                if best is None:
                    return None

                track_id = getattr(best, "id", None)
                if not track_id:
                    return None

                lyr = c.get_lyrics(track_id)
                if not lyr:
                    return None

                lines = getattr(lyr, "lines", None) or []
                sync_type = str(getattr(lyr, "sync_type", "UNSYNCED") or "UNSYNCED")

                timed = _to_timed(lines)
                plain = "\n".join([x["text"] for x in timed]).strip() if timed else None
                if not plain:
                    return None

                # Prefer the ACTUAL matched track's name/artist so downstream validator
                # doesn't see mismatched fields.
                match_name = getattr(best, "name", song) or song
                match_artists_list = []
                try:
                    match_artists_list = [getattr(a, "name", "") or "" for a in (getattr(best, "artists", None) or [])]
                except Exception:
                    match_artists_list = [artist]
                match_artists = ", ".join([a for a in match_artists_list if a]) or artist

                return {
                    "plain": plain,
                    "timed": timed,
                    "sync_type": sync_type,
                    "match_name": match_name,
                    "match_artists": match_artists,
                    "reason": best_reason,
                }

        try:
            out = await loop.run_in_executor(None, _work)
        except Exception:
            return None

        if not out:
            return None

        timed_lyrics = out["timed"] if timestamps else None

        return build_result(
            source="spotify_scraper",
            artist=out.get("match_artists") or artist,
            title=out.get("match_name") or song,
            lyrics=out["plain"],
            timed_lyrics=timed_lyrics,
            has_timestamps=bool(timed_lyrics),
            syncType=out.get("sync_type", "LINE_SYNCED"),
            match_reason=out.get("reason", ""),
        )
