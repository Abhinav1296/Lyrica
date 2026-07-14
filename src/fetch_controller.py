import asyncio
from datetime import datetime, timezone
from src.logger import get_logger
from src.utils import maybe_await
from src.sources import ALL_FETCHERS
from src.validator import validate_lyrics_match

logger = get_logger("fetch_controller")

# IDs stable
FETCHER_MAP = {
    1: "SpotifyScraper",
    2: "Genius",
    3: "LRCLIB",
    4: "YouTube Music",
    5: "NetEase",
    6: "Megalobiz",
    7: "Musixmatch",
    8: "SimpMusic",
    9: "LyricsTape",
}

# Synced-lyrics sequence (primary -> fallbacks)
DEFAULT_SYNCED_SEQUENCE = [1, 3, 4, 5, 6, 7, 8]   # SpotifyScraper, LRCLIB, YouTube, ...
# Plain-lyrics sequence (Genius first, LyricsTape last)
DEFAULT_PLAIN_SEQUENCE  = [2, 3, 4, 5, 6, 7, 8, 9]
# Fast mode: race the top few
FAST_MODE_SEQUENCE      = [1, 3, 4]   # SpotifyScraper + LRCLIB + YouTube


def _registry() -> dict:
    return {
        1: ("SpotifyScraper", ALL_FETCHERS.get("spotify_scraper")),
        2: ("Genius",         ALL_FETCHERS.get("genius")),
        3: ("LRCLIB",         ALL_FETCHERS.get("lrclib")),
        4: ("YouTube Music",  ALL_FETCHERS.get("youtube")),
        5: ("NetEase",        ALL_FETCHERS.get("netease")),
        6: ("Megalobiz",      ALL_FETCHERS.get("megalobiz")),
        7: ("Musixmatch",     ALL_FETCHERS.get("musixmatch")),
        8: ("SimpMusic",      ALL_FETCHERS.get("simpmusic")),
        9: ("LyricsTape",     ALL_FETCHERS.get("lyricstape")),
    }


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _err(msg: str) -> dict:
    return {"status": "error", "error": {"message": msg, "timestamp": _ts()}}


async def fetch_with_timeout(api_name, fetcher, artist, song, timestamps, timeout=12) -> dict:
    try:
        result = await asyncio.wait_for(
            maybe_await(fetcher.fetch, artist, song, timestamps=timestamps),
            timeout=timeout,
        )
        if result and result.get("lyrics"):
            return {"api": api_name, "result": result, "success": True}
        return {"api": api_name, "success": False, "reason": "no_lyrics"}
    except asyncio.TimeoutError:
        return {"api": api_name, "success": False, "reason": "timeout"}
    except Exception as e:
        return {"api": api_name, "success": False, "reason": str(e)}


async def fetch_lyrics_parallel(artist, song, timestamps, fetcher_ids) -> tuple:
    reg = _registry()
    tasks = {}
    for fid in fetcher_ids:
        if fid not in reg:
            continue
        api_name, fetcher = reg[fid]
        if not fetcher:
            continue
        task = asyncio.create_task(fetch_with_timeout(api_name, fetcher, artist, song, timestamps))
        tasks[task] = api_name

    if not tasks:
        return None, []

    attempts = []
    pending = set(tasks.keys())

    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            attempt = await task
            attempts.append(attempt)

            if not attempt.get("success"):
                continue

            val = validate_lyrics_match(artist, song, attempt["result"], threshold=0.75)
            if val["valid"]:
                for p in pending:
                    p.cancel()
                attempt["validation"] = val
                return attempt["result"], attempts

    return None, attempts


async def _run_sequence(artist_name, song_title, timestamps, fetcher_ids, use_parallel: bool) -> dict:
    reg = _registry()

    if use_parallel:
        result, attempts = await fetch_lyrics_parallel(artist_name, song_title, timestamps, fetcher_ids)
        if result:
            return {"status": "success", "data": result}

        sources_with_results = [a["api"] for a in attempts if a.get("success")]
        if sources_with_results:
            return _err(
                f"Found results from {', '.join(sources_with_results)} but none "
                f"matched '{song_title}' by '{artist_name}'"
            )
        return _err(f"No lyrics found for '{song_title}' by '{artist_name}'")

    # sequential path (timeout per fetcher)
    for fid in fetcher_ids:
        if fid not in reg:
            continue
        api_name, fetcher = reg[fid]
        if not fetcher:
            continue

        attempt = await fetch_with_timeout(api_name, fetcher, artist_name, song_title, timestamps, timeout=12)
        if not attempt.get("success"):
            continue

        raw = attempt["result"]
        val = validate_lyrics_match(artist_name, song_title, raw, threshold=0.75)
        if val["valid"]:
            return {"status": "success", "data": raw}

    return _err(f"No lyrics found for '{song_title}' by '{artist_name}'")


async def fetch_lyrics_controller(
    artist_name: str,
    song_title: str,
    timestamps: bool = False,
    pass_param: bool = False,
    sequence: str | None = None,
    fast_mode: bool = False,
) -> dict:
    """
    Hybrid logic:
    - timestamps=true: try synced first (SpotifyScraper -> others)
    - if synced fails: try plain (Genius... -> LyricsTape)
    """

    # custom sequence
    if pass_param and sequence:
        try:
            fetcher_ids = [int(x.strip()) for x in sequence.split(",") if x.strip()]
        except ValueError:
            return _err("Invalid sequence format: must be comma-separated integers")

        use_parallel = len(fetcher_ids) > 1
        res = await _run_sequence(artist_name, song_title, timestamps, fetcher_ids, use_parallel=use_parallel)

        if timestamps and res.get("status") != "success":
            plain = await _run_sequence(artist_name, song_title, False, DEFAULT_PLAIN_SEQUENCE, use_parallel=False)
            if plain.get("status") == "success":
                return plain
        return res

    # fast mode
    if fast_mode:
        res = await _run_sequence(artist_name, song_title, timestamps, FAST_MODE_SEQUENCE, use_parallel=True)
        if timestamps and res.get("status") != "success":
            plain = await _run_sequence(artist_name, song_title, False, DEFAULT_PLAIN_SEQUENCE, use_parallel=False)
            if plain.get("status") == "success":
                return plain
        return res

    # normal mode
    if timestamps:
        synced = await _run_sequence(artist_name, song_title, True, DEFAULT_SYNCED_SEQUENCE, use_parallel=False)
        if synced.get("status") == "success":
            return synced

        plain = await _run_sequence(artist_name, song_title, False, DEFAULT_PLAIN_SEQUENCE, use_parallel=False)
        if plain.get("status") == "success":
            return plain
        return synced

    # plain request
    return await _run_sequence(artist_name, song_title, False, DEFAULT_PLAIN_SEQUENCE, use_parallel=False)
