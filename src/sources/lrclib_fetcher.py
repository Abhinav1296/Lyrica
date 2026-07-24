import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import LRCLIB_API_URL
from src.logger import get_logger
from .base_fetcher import BaseFetcher

logger = get_logger("lrclib_fetcher")


# ------------------------------------------------------------------
# Script ranges (Telugu-first robustness)
# ------------------------------------------------------------------
_TELUGU_RANGE = (0x0C00, 0x0C7F)
_OTHER_INDIC_RANGES = [
    (0x0900, 0x097F),  # Devanagari (Hindi)
    (0x0A00, 0x0A7F),  # Gurmukhi (Punjabi)
    (0x0B80, 0x0BFF),  # Tamil
    (0x0C80, 0x0CFF),  # Kannada
    (0x0D00, 0x0D7F),  # Malayalam
    (0x0980, 0x09FF),  # Bengali
]
_TELUGU_HINTS = ("telugu", "tollywood")
_OTHER_LANG_HINTS = ("hindi", "bollywood", "tamil", "kollywood",
                     "kannada", "sandalwood", "punjabi", "malayalam")


def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def _sim(a: str, b: str) -> float:
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _script_stats(text: str) -> dict:
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
    return {"telugu": telugu, "other_indic": other_indic, "latin": latin, "total": total}


def _candidate_language_signal(name: str, album: str, artist_field: str) -> dict:
    combined_text = " ".join([name or "", album or "", artist_field or ""])
    combined_lower = combined_text.lower()

    telugu_bias = 0.0
    wrong = 0.0

    if any(h in combined_lower for h in _TELUGU_HINTS):
        telugu_bias = max(telugu_bias, 0.9)

    stats = _script_stats(combined_text)
    if stats["telugu"] > 0:
        telugu_bias = 1.0
    if stats["other_indic"] > 0:
        wrong = max(wrong, 0.9)

    for bad in _OTHER_LANG_HINTS:
        if bad in combined_lower:
            wrong = max(wrong, 0.85)

    return {"telugu_bias": telugu_bias, "wrong_lang_bias": wrong}


def _lyrics_language_safety(text: str) -> str:
    """
    Returns:
      'telugu'      -> OK
      'latin'       -> OK (romanized Telugu is common)
      'other_indic' -> REJECT (Hindi/Tamil/etc script content)
      'unknown'     -> treat as OK by default
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


def _make_session() -> requests.Session:
    """
    LRCLIB session with retries + short timeouts + fresh per-call sessions
    to survive dropped keep-alives on their free tier.
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.4,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ------------------------------------------------------------------
# Constants (smart defaults, no env)
# ------------------------------------------------------------------
_MIN_TITLE_SIM       = 0.68
_MIN_ARTIST_SIM      = 0.55
_MIN_COMPOSITE_SCORE = 0.55
_MAX_CANDIDATES      = 8


class LRCLIBFetcher(BaseFetcher):
    """
    LRCLIB fetcher with Telugu-first robustness.

    Strategy:
      1) Try multiple search query variants (Telugu-biased first).
      2) Collect candidates from LRCLIB search results.
      3) Score each candidate by title/artist/telugu-bias, penalize other Indic.
      4) Fetch full track lyrics for best candidates.
      5) Reject if lyrics content is another Indic script (Devanagari/Tamil/etc).
      6) If nothing safe → return None so Lyrica chain falls back further.
    """

    def fetch(self, artist: str, song: str, timestamps: bool = True):
        session = _make_session()
        try:
            logger.info(f"LRCLIB: fetching '{artist} – {song}' (timestamps={timestamps})")

            # ---------- 1) collect candidates from multiple queries ----------
            query_variants = [
                # (track_name, artist_name)
                (f"{song} telugu", artist),
                (song, f"{artist} telugu"),
                (song, artist),
                (song, ""),  # last resort
            ]

            seen_ids = set()
            candidates = []  # list of dicts
            req_artist_norm = _norm(artist)
            req_title_norm  = _norm(song)

            for track_q, artist_q in query_variants:
                params = {"track_name": track_q}
                if artist_q:
                    params["artist_name"] = artist_q

                try:
                    search_resp = session.get(
                        "https://lrclib.net/api/search",
                        params=params,
                        timeout=(5, 15),
                        headers={"User-Agent": "Lyrica/1.0 (music lyrics API)"},
                    )
                except requests.exceptions.RequestException as e:
                    logger.warning(f"LRCLIB search error: {e}")
                    continue

                if search_resp.status_code != 200:
                    logger.warning(f"LRCLIB search {search_resp.status_code} for {params}")
                    continue

                results = search_resp.json() or []
                if not isinstance(results, list):
                    continue

                for r in results[:_MAX_CANDIDATES]:
                    rid = r.get("id")
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)

                    r_title  = r.get("trackName") or ""
                    r_artist = r.get("artistName") or ""
                    r_album  = r.get("albumName") or ""

                    title_sim  = _sim(req_title_norm, r_title)
                    artist_sim = _sim(req_artist_norm, r_artist)
                    lang       = _candidate_language_signal(r_title, r_album, r_artist)

                    score = (
                        0.40 * title_sim +
                        0.25 * artist_sim +
                        0.35 * lang["telugu_bias"] -
                        0.45 * lang["wrong_lang_bias"]
                    )

                    candidates.append({
                        "raw": r,
                        "title_sim":   title_sim,
                        "artist_sim":  artist_sim,
                        "telugu_bias": lang["telugu_bias"],
                        "wrong_lang":  lang["wrong_lang_bias"],
                        "score":       score,
                    })

                if len(candidates) >= _MAX_CANDIDATES:
                    break

            if not candidates:
                logger.info("LRCLIB: no candidates from any query variant")
                return None

            # ---------- 2) sort best-first ----------
            candidates.sort(key=lambda x: x["score"], reverse=True)

            # ---------- 3) try lyrics for top candidates with gates ----------
            rejected = []
            for cand in candidates[:_MAX_CANDIDATES]:
                if cand["title_sim"] < _MIN_TITLE_SIM:
                    rejected.append(("low_title_sim", cand))
                    continue

                if cand["artist_sim"] < _MIN_ARTIST_SIM and cand["telugu_bias"] < 0.5:
                    rejected.append(("weak_artist_and_no_telugu", cand))
                    continue

                if cand["wrong_lang"] >= 0.85 and cand["telugu_bias"] < 0.9:
                    rejected.append(("wrong_lang_metadata", cand))
                    continue

                if cand["score"] < _MIN_COMPOSITE_SCORE:
                    rejected.append(("low_composite_score", cand))
                    continue

                raw = cand["raw"]
                try:
                    get_resp = session.get(
                        LRCLIB_API_URL,
                        params={
                            "track_name":  raw.get("trackName"),
                            "artist_name": raw.get("artistName"),
                            "album_name":  raw.get("albumName"),
                            "duration":    raw.get("duration"),
                        },
                        timeout=(5, 15),
                        headers={"User-Agent": "Lyrica/1.0 (music lyrics API)"},
                    )
                except requests.exceptions.RequestException as e:
                    rejected.append(("get_error", cand))
                    logger.warning(f"LRCLIB get error: {e}")
                    continue

                if get_resp.status_code != 200:
                    rejected.append(("get_status", cand))
                    continue

                data = get_resp.json() or {}
                lyrics_text = data.get("syncedLyrics") if timestamps else data.get("plainLyrics")
                if not lyrics_text:
                    if timestamps:
                        lyrics_text = data.get("plainLyrics")
                    if not lyrics_text:
                        rejected.append(("no_lyrics_body", cand))
                        continue

                # ---------- 4) POST-FETCH language safety ----------
                lang_verdict = _lyrics_language_safety(lyrics_text)
                if lang_verdict == "other_indic":
                    rejected.append(("wrong_lang_lyrics", cand))
                    continue
                # telugu / latin / unknown → accept

                # Build result (same schema as before)
                result = {
                    "source":       "lrclib",
                    "artist":       data.get("artistName"),
                    "title":        data.get("trackName"),
                    "album":        data.get("albumName"),
                    "duration":     data.get("duration"),
                    "instrumental": data.get("instrumental", False),
                    "lyrics":       lyrics_text,
                    "hasTimestamps": False,
                    "timestamp":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "match_reason": (
                        f"title_sim={cand['title_sim']:.2f} "
                        f"artist_sim={cand['artist_sim']:.2f} "
                        f"telugu={cand['telugu_bias']:.2f} "
                        f"wrong_lang={cand['wrong_lang']:.2f} "
                        f"score={cand['score']:.2f} "
                        f"lang_verdict={lang_verdict}"
                    ),
                    "match_rejected": len(rejected),
                }

                # Parse synced lyrics if we have them
                if timestamps and data.get("syncedLyrics"):
                    timed = _parse_lrc(data["syncedLyrics"], data.get("duration"))
                    if timed:
                        result["timed_lyrics"]  = timed
                        result["hasTimestamps"] = True

                logger.info(
                    f"LRCLIB accepted: '{result['title']}' by '{result['artist']}' "
                    f"({result['match_reason']})"
                )
                return result

            logger.info(
                f"LRCLIB: no Telugu-safe candidate accepted "
                f"(evaluated={len(candidates)}, rejected={len(rejected)})"
            )
            return None

        except Exception as e:
            logger.error(f"LRCLIB error: {e}")
            return None
        finally:
            session.close()


def _parse_lrc(synced_lyrics: str, duration=None) -> list:
    """Parse LRC-format synced lyrics into a list of timed dicts."""
    timed = []
    lines = synced_lyrics.split("\n")
    pattern = re.compile(r"\[(\d{2}:\d{2}\.?\d{1,3})\](.*)")

    for i, line in enumerate(lines):
        m = pattern.match(line)
        if not m:
            continue
        time_str, text = m.group(1), m.group(2)
        time_str = time_str.replace("..", ".")
        try:
            parts = time_str.split(":")
            minutes = float(parts[0])
            seconds = float(parts[1])
            start_ms = int((minutes * 60 + seconds) * 1000)

            end_ms = None
            if i + 1 < len(lines):
                nm = pattern.match(lines[i + 1])
                if nm:
                    nt = nm.group(1).replace("..", ".")
                    np = nt.split(":")
                    nm_sec = float(np[0]) * 60 + float(np[1])
                    end_ms = int(nm_sec * 1000)

            if end_ms is None:
                end_ms = (
                    int(duration * 1000) if duration
                    else start_ms + 4000
                )

            if text.strip():
                timed.append({
                    "text":       text.strip(),
                    "start_time": start_ms,
                    "end_time":   end_ms,
                    "id":         f"lrc_{i}",
                })
        except (ValueError, IndexError):
            continue

    return timed
