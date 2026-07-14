import os
import json
import hashlib
import threading
from time import time
from typing import Optional, Callable, Any, Tuple
from src.config import CACHE_DIR, CACHE_TTL

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_VERSION = "v3"  # bump if response format changes

# -----------------------------
# In-memory TTL cache overlay
# -----------------------------
_MEM: dict[str, tuple[float, Any]] = {}          # key -> (expiry_epoch, result)
_MEM_LOCK = threading.Lock()

# In-flight coalescing: key -> Event
_INFLIGHT: dict[str, threading.Event] = {}
_INFLIGHT_LOCK = threading.Lock()


def make_cache_key(
    artist: str,
    song: str,
    timestamps: bool,
    sequence: Optional[str],
    fast: bool,
    mood: bool,
    metadata: bool
) -> str:
    payload = {
        "v": CACHE_VERSION,
        "artist": (artist or "").strip().lower(),
        "song": (song or "").strip().lower(),
        "timestamps": bool(timestamps),
        "sequence": sequence or "",
        "fast": bool(fast),
        "mood": bool(mood),
        "metadata": bool(metadata),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_cache_path(key: str) -> str:
    return os.path.join(CACHE_DIR, f"{key}.json")


def _mem_get(key: str):
    now = time()
    with _MEM_LOCK:
        entry = _MEM.get(key)
        if not entry:
            return None
        expiry, result = entry
        if now > expiry:
            _MEM.pop(key, None)
            return None
        return result


def _mem_set(key: str, result, ttl: int = CACHE_TTL):
    with _MEM_LOCK:
        _MEM[key] = (time() + ttl, result)


def load_from_cache(key: str):
    # 1) Memory
    mem = _mem_get(key)
    if mem is not None:
        return mem

    # 2) Disk
    path = _get_cache_path(key)
    if not os.path.exists(path):
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if time() > data.get("expiry", 0):
            try:
                os.remove(path)
            except Exception:
                pass
            return None

        result = data.get("result")
        if result is not None:
            _mem_set(key, result)
        return result

    except Exception:
        # corrupted cache entry → delete
        try:
            os.remove(path)
        except Exception:
            pass
        return None


def save_to_cache(key: str, result):
    # memory first
    _mem_set(key, result)

    # disk second (best-effort)
    path = _get_cache_path(key)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"expiry": time() + CACHE_TTL, "result": result},
                f,
                ensure_ascii=False,
                separators=(",", ":"),
            )
    except Exception:
        pass


def get_or_fetch_coalesced(
    key: str,
    fetch_func: Callable[[], Any],
    should_cache: Optional[Callable[[Any], bool]] = None,
) -> Tuple[Any, bool]:
    """
    Returns: (result, cache_hit)

    - HIT: returned from memory/disk cache immediately
    - MISS: leader computes fetch_func
    - COALESCED: followers wait for leader; then read cache
    """
    cached = load_from_cache(key)
    if cached is not None:
        return cached, True

    # In-flight coalescing
    with _INFLIGHT_LOCK:
        ev = _INFLIGHT.get(key)
        if ev is None:
            ev = threading.Event()
            _INFLIGHT[key] = ev
            leader = True
        else:
            leader = False

    if not leader:
        # follower waits, then tries cache again
        ev.wait(timeout=90)
        cached2 = load_from_cache(key)
        if cached2 is not None:
            return cached2, True
        # leader failed or didn't cache; fallback compute
        result = fetch_func()
        return result, False

    try:
        result = fetch_func()
        ok = True if should_cache is None else bool(should_cache(result))
        if ok:
            save_to_cache(key, result)
        return result, False
    finally:
        with _INFLIGHT_LOCK:
            ev.set()
            _INFLIGHT.pop(key, None)


def clear_cache():
    removed, failed = [], []

    # memory
    with _MEM_LOCK:
        _MEM.clear()

    # disk
    for fname in os.listdir(CACHE_DIR):
        path = os.path.join(CACHE_DIR, fname)
        try:
            os.remove(path)
            removed.append(fname)
        except Exception as e:
            failed.append({"file": fname, "error": str(e)})

    return {"removed": removed, "failed": failed, "memory_cleared": True}


def cache_stats():
    files = []
    try:
        files = os.listdir(CACHE_DIR)
    except Exception:
        files = []
    with _MEM_LOCK:
        mem_entries = len(_MEM)
    with _INFLIGHT_LOCK:
        inflight = len(_INFLIGHT)

    return {
        "cache_dir": CACHE_DIR,
        "disk_cache_files": len(files),
        "ttl_seconds": CACHE_TTL,
        "version": CACHE_VERSION,
        "memory_entries": mem_entries,
        "inflight_keys": inflight,
    }
