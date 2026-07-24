import os
import re
import asyncio
import unicodedata
from difflib import SequenceMatcher

from .base_fetcher import BaseFetcher, build_result


# ------------------------------------------------------------------
# Script ranges
# ------------------------------------------------------------------
# Telugu script
_TELUGU_RANGE = (0x0C00, 0x0C7F)
# Other Indic scripts we treat as "wrong language" signal for Telugu-first app
_OTHER_INDIC_RANGES = [
    (0x0900, 0x097F),  # Devanagari (Hindi/Marathi/Sanskrit)
    (0x0A00, 0x0A7F),  # Gurmukhi (Punjabi)
    (0x0B80, 0x0BFF),  # Tamil
    (0x0C80, 0x0CFF),  # Kannada
    (0x0D00, 0x0D7F),  # Malayalam
    (0x0980, 0x09FF),  # Bengali
]
_TELUGU_HINTS = ("telugu", "tollywood")


def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def _script_stats(text: str) -> dict:
    """
    Count characters per relevant script.
    """
    telugu = 0
    other_indic = 0
    latin = 0
    total = 0
    if not text:
        return {"telugu": 0, "other_indic": 0, "latin": 0, "total": 0}

    for ch in text:
        cp = ord(ch)
        if _TELUGU_RANGE[0] <= cp <= _TELUGU_RANGE[1]:
            telugu += 1
            total += 1
            continue
        matched_other = False
        for lo, hi in _OTHER_INDIC_RANGES:
            if lo <= cp <= hi:
                other_indic += 1
                total += 1
                matched_other = True
                break
        if matched_other:
            continue
        if (0x41 <= cp <= 0x5A) or (0x61 <= cp <= 0x7A):
            latin += 1
            total += 1
            continue
        # ignore spaces, digits, punctuation
    return {"telugu": telugu, "other_indic": other_indic, "latin": latin, "total": total}


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
    if not req_artists or not cand_artists:
        return 0.0
    best = 0.0
    for r in req_artists:
        for c in cand_artists:
            best = max(best, _sim(r, c))
            if len(r) >= 3 and r in c:
                best = max(best, 0.95)
            if len(c) >= 3 and c in r:
                best = max(best, 0.9)
    return best


def _candidate_language_signal(name: str, album: str, artists_names) -> dict:
    """
    Determine language signal from Spotify metadata (title/album/artists).
    Returns:
        {
            "telugu_bias": 0..1,        # positive signal
            "wrong_lang_bias": 0..1,    # negative signal (other Indic scripts / Hindi/Tamil album hints)
        }
    """
    combined_text = " ".join([name or "", album or "", " ".join(artists_names or [])])
    combined_lower = combined_text.lower()

    telugu_bias = 0.0
    wrong = 0.0

    # positive signals
    if any(h in combined_lower for h in _TELUGU_HINTS):
        telugu_bias = max(telugu_bias, 0.9)

    stats = _script_stats(combined_text)
    if stats["telugu"] > 0:
        telugu_bias = 1.0
    if stats["other_indic"] > 0:
        wrong = max(wrong, 0.9)

    # explicit "hindi"/"tamil"/... in album/title text
    for bad in ("hindi", "bollywood", "tamil", "kollywood", "kannada", "sandalwood", "punjabi", "malayalam"):
        if bad in combined_lower:
            wrong = max(wrong, 0.85)

    return {"telugu_bias": telugu_bias, "wrong_lang_bias": wrong}


def _lyrics_language_safety(text: str) -> str:
    """
    Post-fetch check on returned lyrics.
    Returns:
        "telugu"      -> OK
        "latin"       -> OK (romanized telugu is common on Spotify)
        "other_indic" -> NOT OK, reject
        "unknown"     -> treat as OK by default (don't over-reject)
    """
    if not text:
        return "unknown"
    stats = _script_stats(text)
    if stats["telugu"] >= 5:
        return "telugu"
    if stats["other_indic"] >= 10 and stats["other_indic"] > stats["telugu"] * 2:
        return "other_indic"
    if stats["latin"] >= 20 and stats["other_indic"] == 0:
        return "latin"
    return "unknown"


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
    Primary synced lyrics source using SpotifyScraper — Telugu-first robust matcher.
    Requires env var: SPOTIFY_SP_DC

    Strategy:
      1) Multiple Spotify search queries (Telugu-biased first).
      2) Score each candidate on title/artist/telugu-bias/wrong-lang-penalty.
      3) Try lyrics of the best candidate. If content looks like another Indic
         script (Hindi/Tamil/Kannada/etc.), reject and try the next candidate.
      4) If everything fails safety -> return None so Lyrica falls back to
         LRCLIB / YouTube / LyricsTape.
    """
    source_name = "spotify_scraper"

    # Smart defaults (no env needed)
    _MIN_TITLE_SIM       = 0.68
    _MIN_ARTIST_SIM      = 0.65
    _MIN_TELUGU_BIAS     = 0.50
    _MIN_COMPOSITE_SCORE = 0.55

    async def fetch(self, artist: str, song: str, timestamps: bool = False):
        sp_dc = (os.getenv("SPOTIFY_SP_DC") or "").strip()
        if not sp_dc:
            return None

        loop = asyncio.get_event_loop()

        req_artists   = _split_artists(artist)
        req_title_norm = _norm(song)

        def _work():
            from spotify_scraper import SpotifyClient

            query_variants = [
                f'track:"{song}" artist:"{artist}" telugu',
                f'track:"{song}" telugu',
                f'{song} {artist} telugu',
                f'{song} telugu',
                f'track:"{song}" artist:"{artist}"',
            ]

            with SpotifyClient(cookies={"sp_dc": sp_dc}) as c:
                # 1) Collect a pool of candidates from multiple queries
                seen_ids = set()
                candidates = []  # list of dicts with metadata + score

                for q in query_variants:
                    try:
                        res = c.search(q, types=("track",), limit=5)
                    except Exception:
                        continue
                    if not res or not getattr(res, "tracks", None):
                        continue

                    for t in res.tracks[:5]:
                        tid = getattr(t, "id", None)
                        if not tid or tid in seen_ids:
                            continue
                        seen_ids.add(tid)

                        t_name = getattr(t, "name", "") or ""
                        try:
                            t_album = getattr(t, "album", None) and getattr(t.album, "name", "") or ""
                        except Exception:
                            t_album = ""
                        try:
                            t_artist_objs = getattr(t, "artists", None) or []
                            t_artist_names = [getattr(a, "name", "") or "" for a in t_artist_objs]
                        except Exception:
                            t_artist_names = []

                        title_sim = _sim(req_title_norm, t_name)
                        artist_sim = _artist_overlap_score(req_artists, [_norm(x) for x in t_artist_names])
                        lang = _candidate_language_signal(t_name, t_album, t_artist_names)
                        telugu_bias = lang["telugu_bias"]
                        wrong_lang = lang["wrong_lang_bias"]

                        # Composite score. Heavy weight on Telugu, penalize other-lang.
                        score = (
                            0.40 * title_sim +
                            0.25 * artist_sim +
                            0.35 * telugu_bias -
                            0.45 * wrong_lang
                        )

                        candidates.append({
                            "track": t,
                            "id": tid,
                            "name": t_name,
                            "album": t_album,
                            "artists": t_artist_names,
                            "title_sim": title_sim,
                            "artist_sim": artist_sim,
                            "telugu_bias": telugu_bias,
                            "wrong_lang": wrong_lang,
                            "score": score,
                            "query": q,
                        })

                if not candidates:
                    return None

                # 2) Sort candidates: best score first
                candidates.sort(key=lambda x: x["score"], reverse=True)

                # 3) Try lyrics for top N candidates, applying hard gates + safety
                rejected = []
                for cand in candidates[:6]:
                    # Hard gates before we even hit lyrics endpoint
                    if cand["title_sim"] < SpotifyScraperFetcher._MIN_TITLE_SIM:
                        rejected.append(("low_title_sim", cand))
                        continue
                    if cand["artist_sim"] < SpotifyScraperFetcher._MIN_ARTIST_SIM and cand["telugu_bias"] < SpotifyScraperFetcher._MIN_TELUGU_BIAS:
                        rejected.append(("weak_artist_and_no_telugu", cand))
                        continue
                    if cand["wrong_lang"] >= 0.85 and cand["telugu_bias"] < 0.9:
                        # explicit non-telugu signal wins → skip
                        rejected.append(("wrong_lang_metadata", cand))
                        continue
                    if cand["score"] < SpotifyScraperFetcher._MIN_COMPOSITE_SCORE:
                        rejected.append(("low_composite_score", cand))
                        continue

                    # 4) Fetch lyrics
                    try:
                        lyr = c.get_lyrics(cand["id"])
                    except Exception:
                        rejected.append(("lyrics_exception", cand))
                        continue
                    if not lyr:
                        rejected.append(("no_lyrics", cand))
                        continue

                    lines = getattr(lyr, "lines", None) or []
                    if not lines:
                        rejected.append(("empty_lines", cand))
                        continue

                    sync_type = str(getattr(lyr, "sync_type", "UNSYNCED") or "UNSYNCED")

                    timed = _to_timed(lines)
                    plain = "\n".join([x["text"] for x in timed]).strip() if timed else None
                    if not plain:
                        rejected.append(("empty_plain", cand))
                        continue

                    # 5) POST-FETCH language safety check
                    lang_verdict = _lyrics_language_safety(plain)
                    if lang_verdict == "other_indic":
                        rejected.append(("wrong_lang_lyrics", cand))
                        continue
                    # telugu / latin / unknown → accept

                    return {
                        "plain": plain,
                        "timed": timed,
                        "sync_type": sync_type,
                        "match_name": cand["name"] or song,
                        "match_artists": ", ".join([a for a in cand["artists"] if a]) or artist,
                        "reason": (
                            f"query='{cand['query']}' "
                            f"title_sim={cand['title_sim']:.2f} "
                            f"artist_sim={cand['artist_sim']:.2f} "
                            f"telugu={cand['telugu_bias']:.2f} "
                            f"wrong_lang={cand['wrong_lang']:.2f} "
                            f"score={cand['score']:.2f} "
                            f"lang_verdict={lang_verdict}"
                        ),
                        "rejected_count": len(rejected),
                    }

                # Everything rejected → return None so fallback sources try
                return None

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
            match_rejected=out.get("rejected_count", 0),
        )
