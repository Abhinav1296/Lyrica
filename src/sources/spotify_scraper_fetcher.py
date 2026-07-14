import os
import asyncio
from typing import Optional

from .base_fetcher import BaseFetcher, build_result


def _to_timed(lines) -> list:
    """
    Convert SpotifyScraper lyrics lines -> Lyrica timed_lyrics format.
    Each line has: start_ms, text
    """
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

    # fill end_time from next line start
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
    """
    source_name = "spotify_scraper"

    async def fetch(self, artist: str, song: str, timestamps: bool = False):
        sp_dc = (os.getenv("SPOTIFY_SP_DC") or "").strip()
        if not sp_dc:
            return None

        # SpotifyScraper is sync-style; run in a thread so we don't block the event loop
        loop = asyncio.get_event_loop()

        def _work():
            from spotify_scraper import SpotifyClient

            with SpotifyClient(cookies={"sp_dc": sp_dc}) as c:
                # Search track
                res = c.search(f"{song} {artist}", types=("track",), limit=3)
                if not res or not getattr(res, "tracks", None):
                    return None

                track = res.tracks[0]
                track_id = getattr(track, "id", None)
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

                return {
                    "plain": plain,
                    "timed": timed,
                    "sync_type": sync_type,
                }

        try:
            out = await loop.run_in_executor(None, _work)
        except Exception:
            return None

        if not out:
            return None

        # Respect timestamps param: include timed_lyrics only if requested
        timed_lyrics = out["timed"] if timestamps else None

        return build_result(
            source="spotify_scraper",
            artist=artist,
            title=song,
            lyrics=out["plain"],
            timed_lyrics=timed_lyrics,
            has_timestamps=bool(timed_lyrics),
            syncType=out.get("sync_type", "LINE_SYNCED"),
        )
