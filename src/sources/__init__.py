# src/sources/__init__.py
ALL_FETCHERS = {}

def _safe(name: str, cls):
    try:
        ALL_FETCHERS[name] = cls()
    except Exception:
        ALL_FETCHERS[name] = None


# Import fetchers (safe)
try:
    from .genius_fetcher import GeniusFetcher
    _safe("genius", GeniusFetcher)
except Exception:
    ALL_FETCHERS["genius"] = None

try:
    from .lrclib_fetcher import LRCLIBFetcher
    _safe("lrclib", LRCLIBFetcher)
except Exception:
    ALL_FETCHERS["lrclib"] = None

try:
    from .youtube_fetcher import YoutubeFetcher
    _safe("youtube", YoutubeFetcher)
except Exception:
    ALL_FETCHERS["youtube"] = None

try:
    from .netease_fetcher import NetEaseFetcher
    _safe("netease", NetEaseFetcher)
except Exception:
    ALL_FETCHERS["netease"] = None

try:
    from .megalobiz_fetcher import MegalobizFetcher
    _safe("megalobiz", MegalobizFetcher)
except Exception:
    ALL_FETCHERS["megalobiz"] = None

try:
    from .musixmatch_fetcher import MusixmatchFetcher
    _safe("musixmatch", MusixmatchFetcher)
except Exception:
    ALL_FETCHERS["musixmatch"] = None

try:
    from .simp_music_fetcher import SimpMusicFetcher
    _safe("simpmusic", SimpMusicFetcher)
except Exception:
    ALL_FETCHERS["simpmusic"] = None

# NEW: LyricsTape (plain-only fallback)
try:
    from .lyricstape_fetcher import LyricsTapeFetcher
    _safe("lyricstape", LyricsTapeFetcher)
except Exception:
    ALL_FETCHERS["lyricstape"] = None
