import asyncio
from datetime import datetime, timezone
from src.logger import get_logger
from src.utils import maybe_await
from src.sources import ALL_FETCHERS
from src.validator import validate_lyrics_match

logger = get_logger("fetch_controller")

FETCHER_MAP = {
    1: "Genius",
    2: "LRCLIB",
    3: "YouTube Music",
    4: "NetEase",
    5: "Megalobiz",
    6: "Musixmatch",
    7: "SimpMusic",
}

DEFAULT_SYNCED_SEQUENCE = [2, 3, 4, 5, 6, 7]
DEFAULT_PLAIN_SEQUENCE  = [1, 2, 3, 4, 5, 6, 7]
FAST_MODE_SEQUENCE      = [2, 3]


def _registry() -> dict:
    return {
        1: ("Genius",        ALL_FETCHERS.get("genius")),
        2: ("LRCLIB",        ALL_FETCHERS.get("lrclib")),
        3: ("YouTube Music", ALL_FETCHERS.get("youtube")),
        4: ("NetEase",       ALL_FETCHERS.get("netease")),
        5: ("Megalobiz",     ALL_FETCHERS.get("megalobiz")),
        6: ("Musixmatch",    ALL_FETCHERS.get("musixmatch")),
        7: ("SimpMusic",     ALL_FETCHERS.get("simpmusic")),
    }


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _err(msg: str) -> dict:
    return {"status": "error", "error": {"message": msg, "timestamp": _ts()}}


async def fetch_with_timeout(
    api_name: str,
    fetcher,
    artist: str,
    song: str,
    timestamps: bool,
    timeout: int = 12,
) -> dict:
    try:
        result = await asyncio.wait_for(
            maybe_await(fetcher.fetch, artist, song, timestamps=timestamps),
            timeout=timeout,
        )
        if result and result.get("lyrics"):
            return {"api": api_name, "result": result, "success": True}
        return {"api": api_name, "success": False, "reason": "no_lyrics"}

    except asyncio.TimeoutError:
        logger.warning(f"[{api_name}] timed out after {timeout}s")
        return {"api": api_name, "success": False, "reason": "timeout"}
    except Exception as e:
        logger.error(f"[{api_name}] error: {e}")
        return {"api": api_name, "success": False, "reason": str(e)}


async def fetch_lyrics_parallel(
    artist: str,
    song: str,
    timestamps: bool,
    fetcher_ids: list,
) -> tuple:
    reg = _registry()
    tasks = {}

    for fid in fetcher_ids:
        if fid not in reg:
            continue
        api_name, fetcher = reg[fid]
        if not fetcher:
            continue
        task = asyncio.create_task(
            fetch_with_timeout(api_name, fetcher, artist, song, timestamps)
        )
        tasks[task] = api_name

    if not tasks:
        return None, []

    all_attempts = []
    pending = set(tasks.keys())

    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            attempt = await task
            all_attempts.append(attempt)

            if not attempt["success"]:
                continue

            val = validate_lyrics_match(artist, song, attempt["result"], threshold=0.75)

            if val["valid"]:
                logger.info(
                    f"✓ [{attempt['api']}] accepted "
                    f"(artist={val['artist_match']} song={val['song_match']} "
                    f"script_mismatch={val['script_mismatch']})"
                )
                for p in pending:
                    p.cancel()
                attempt["validation"] = val
                return attempt["result"], all_attempts

            logger.warning(
                f"✗ [{attempt['api']}] rejected: {val['reason']} "
                f"— {len(pending)} fetcher(s) still running"
            )

    logger.warning(f"All fetchers exhausted — no valid result for '{artist} - {song}'")
    return None, all_attempts


async def fetch_lyrics_controller(
    artist_name: str,
    song_title: str,
    timestamps: bool = False,
    pass_param: bool = False,
    sequence: str | None = None,
    fast_mode: bool = False,
) -> dict:
    if fast_mode:
        fetcher_ids = FAST_MODE_SEQUENCE
        use_parallel = True
        logger.info(f"Fast mode: '{artist_name} - {song_title}'")

    elif pass_param and sequence:
        try:
            fetcher_ids = [int(x.strip()) for x in sequence.split(",") if x.strip()]
        except ValueError:
            return _err("Invalid sequence format: must be comma-separated integers")

        _max_id = max(FETCHER_MAP.keys()) if FETCHER_MAP else 6
        if (
            not fetcher_ids
            or not all(1 <= x <= _max_id for x in fetcher_ids)
            or len(fetcher_ids) > _max_id
            or len(fetcher_ids) != len(set(fetcher_ids))
        ):
            return _err(f"Invalid sequence: must be unique numbers between 1 and {_max_id}")

        use_parallel = len(fetcher_ids) > 1

    else:
        fetcher_ids = DEFAULT_SYNCED_SEQUENCE if timestamps else DEFAULT_PLAIN_SEQUENCE
        use_parallel = False

    # Parallel path
    if use_parallel:
        result, attempts = await fetch_lyrics_parallel(artist_name, song_title, timestamps, fetcher_ids)

        if result:
            response = {"status": "success", "data": result}
            for a in attempts:
                if a.get("result") is result and a.get("validation"):
                    v = a["validation"]
                    if v["artist_match"] < 1.0 or v["song_match"] < 1.0:
                        response["validation"] = {
                            k: v[k] for k in ("artist_match", "song_match", "reason", "script_mismatch")
                        }
                    break
            return response

        sources_with_results = [a["api"] for a in attempts if a.get("success")]
        if sources_with_results:
            return _err(
                f"Found results from {', '.join(sources_with_results)} but none "
                f"matched '{song_title}' by '{artist_name}'"
            )
        return _err(f"No lyrics found for '{song_title}' by '{artist_name}'")

    # Sequential path (FIX: add timeouts per fetcher)
    reg = _registry()
    attempts = []

    for fid in fetcher_ids:
        if fid not in reg:
            continue
        api_name, fetcher = reg[fid]

        if not fetcher:
            attempts.append({"api": api_name, "status": "not_configured"})
            continue

        attempt = await fetch_with_timeout(api_name, fetcher, artist_name, song_title, timestamps, timeout=12)

        if not attempt.get("success"):
            attempts.append({"api": api_name, "status": attempt.get("reason", "failed")})
            continue

        raw = attempt["result"]
        val = validate_lyrics_match(artist_name, song_title, raw, threshold=0.75)

        if val["valid"]:
            logger.info(f"✓ [{api_name}] accepted (artist={val['artist_match']} song={val['song_match']})")
            return {"status": "success", "data": raw}

        logger.warning(f"✗ [{api_name}] rejected: {val['reason']} — trying next fetcher")
        attempts.append({"api": api_name, "status": "validation_failed", "reason": val["reason"]})

    return _err(f"No lyrics found for '{song_title}' by '{artist_name}'")
