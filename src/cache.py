import os
import json
import hashlib
import threading
from time import time
from typing import Optional, Callable, Any, Tuple

from src.config import CACHE_DIR, CACHE_TTL

# Ensure cache directory exists (disk cache is optional on Render but harmless)
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_VERSION = "v4"  # bump if response format changes

# -----------------------------
# In-memory TTL cache (L1)
# -----------------------------
_MEM: dict[str, tuple[float, Any]] = {}          # key -> (expiry_epoch, result)
_MEM_LOCK = threading.Lock()

# In-flight coalescing: key -> Event (prevents thundering herd per instance)
_INFLIGHT: dict[str, threading.Event] = {}
_INFLIGHT_LOCK = threading.Lock()

# -----------------------------
# Redis (L2) — optional
# -----------------------------
_REDIS = None
_REDIS_INIT_LOCK = threading.Lock()
_REDIS_ENABLED = (os.getenv("REDIS_ENABLED", "true").lower() == "true")
_REDIS_URL = os.getenv("REDIS_URL", "").strip()
_REDIS_PREFIX = os.getenv("REDIS_PREFIX", "lyrica:cache:")

def _redis_client():
    """
    Lazy init a global redis client (connection pool).
    Never throws: returns None if redis is not configured/available.
    """
    global _REDIS
    if not _REDIS_ENABLED or not _REDIS_URL:
        return None

    if _REDIS is not None:
        return _REDIS

    with _REDIS_INIT_LOCK:
        if _REDIS is not None:
            return _REDIS
        try:
            import redis
            # Upstash uses rediss:// (TLS). decode_responses=True returns strings.
            _REDIS = redis.Redis.from_url(
                _REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                retry_on_timeout=True,
            )
            # quick ping (best-effort)
            _REDIS.ping()
            return _REDIS
        except Exception:
            _REDIS = None
            return None


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


def _disk_path(key: str) -> str:
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


def _redis_key(key: str) -> str:
    return f"{_REDIS_PREFIX}{key}"


def _redis_get(key: str):
    r = _redis_client()
    if not r:
        return None
    try:
        raw = r.get(_redis_key(key))
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def _redis_set(key: str, result, ttl: int = CACHE_TTL):
    r = _redis_client()
    if not r:
        return
    try:
        raw = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        # setex = set with expiry
        r.setex(_redis_key(key), int(ttl), raw)
    except Exception:
        pass


def _disk_get(key: str):
    path = _disk_path(key)
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
        return data.get("result")
    except Exception:
        try:
            os.remove(path)
        except Exception:
            pass
        return None


def _disk_set(key: str, result, ttl: int = CACHE_TTL):
    path = _disk_path(key)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"expiry": time() + ttl, "result": result},
                f,
                ensure_ascii=False,
                separators=(",", ":"),
            )
    except Exception:
        pass


def load_from_cache(key: str):
    # L1: memory
    mem = _mem_get(key)
    if mem is not None:
        return mem

    # L2: redis (persistent across restarts)
    red = _redis_get(key)
    if red is not None:
        _mem_set(key, red)
        return red

    # L3: disk (best-effort)
    disk = _disk_get(key)
    if disk is not None:
        _mem_set(key, disk)
        # also backfill redis so next time survives restart
        _redis_set(key, disk)
        return disk

    return None


def save_to_cache(key: str, result):
    # Memory first
    _mem_set(key, result)

    # Redis second (persistent)
    _redis_set(key, result)

    # Disk last (best-effort)
    _disk_set(key, result)


def get_or_fetch_coalesced(
    key: str,
    fetch_func: Callable[[], Any],
    should_cache: Optional[Callable[[Any], bool]] = None,
) -> Tuple[Any, bool]:
    """
    Returns: (result, cache_hit)

    - HIT: returned from memory/redis/disk immediately
    - MISS: leader computes fetch_func
    - COALESCED: followers wait for leader; then read cache
    """
    cached = load_from_cache(key)
    if cached is not None:
        return cached, True

    # In-flight coalescing (per instance)
    with _INFLIGHT_LOCK:
        ev = _INFLIGHT.get(key)
        if ev is None:
            ev = threading.Event()
            _INFLIGHT[key] = ev
            leader = True
        else:
            leader = False

    if not leader:
        ev.wait(timeout=120)
        cached2 = load_from_cache(key)
        if cached2 is not None:
            return cached2, True
        # leader failed / didn't cache -> last resort compute
        return fetch_func(), False

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

    # redis (only keys with prefix — safest approach is: do nothing unless you really need it)
    # We won't scan/delete by prefix here to avoid heavy operations on free tier.
    # If you need full Redis clear, do it from Upstash dashboard.

    # disk
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            try:
                os.remove(path)
                removed.append(fname)
            except Exception as e:
                failed.append({"file": fname, "error": str(e)})
    except Exception:
        pass

    return {"removed": removed, "failed": failed, "memory_cleared": True, "redis_cleared": False}


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

    r = _redis_client()
    redis_ok = bool(r)

    return {
        "cache_dir": CACHE_DIR,
        "disk_cache_files": len(files),
        "ttl_seconds": CACHE_TTL,
        "version": CACHE_VERSION,
        "memory_entries": mem_entries,
        "inflight_keys": inflight,
        "redis_enabled": bool(_REDIS_URL) and _REDIS_ENABLED,
        "redis_ok": redis_ok,
        "redis_prefix": _REDIS_PREFIX,
    }
