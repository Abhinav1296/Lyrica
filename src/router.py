from flask import jsonify, request, render_template
from datetime import datetime, timezone
import os
import asyncio
import httpx as _httpx
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

from src.logger import get_logger
from src.cache import (
    make_cache_key,
    load_from_cache,
    save_to_cache,
    clear_cache,
    cache_stats,
    get_or_fetch_coalesced,
)
from src.fetch_controller import fetch_lyrics_controller
from src.sentiment_analyzer import analyze_sentiment, analyze_word_frequency, extract_lyrics_text
from src.metadata_extractor import get_metadata_only
from src.sources.jiosaavan_fetcher import search_jiosaavn, get_jiosaavn_stream
from src.trending_analytics import TrendingAnalyticsEngine, Country
from src.config import ADMIN_KEY

logger = get_logger("router")

# Initialize Trending Analytics Engine (global instance)
trending_engine = TrendingAnalyticsEngine(cache_ttl_hours=24)

# Background executor for metadata (best-effort)
_META_BG_EXEC = ThreadPoolExecutor(max_workers=2)

# Metadata time budget (seconds). Keep low so /lyrics doesn't stall.
META_BUDGET_SECONDS = float(os.getenv("META_BUDGET_SECONDS", "4.0"))


def run_async(coro, timeout=30):
    """Run async coroutine safely in sync context with timeout"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(coro)
        return loop.run_until_complete(asyncio.wait_for(coro, timeout=timeout))
    except asyncio.TimeoutError:
        logger.error("Async operation timed out")
        raise Exception("Request timed out - operation took too long")
    except RuntimeError:
        return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


def register_routes(app):
    @app.route("/")
    def home():
        return jsonify(
            {
                "api": "Lyrica",
                "version": app.config.get("VERSION", "1.0.0"),
                "status": "active",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    @app.route("/lyrics/", methods=["GET"])
    def lyrics():
        """
        Hybrid model:
        - Try synced/regular sources via fetch_lyrics_controller
        - If no lyrics: try plain fallback inside controller (including LyricsTape)
        - If still no lyrics:
            - If stream exists -> return 200 status=partial
            - Else -> return 404 status=error
        NEVER return 500 for normal "not found".
        """
        rid = str(uuid.uuid4())[:8]
        t0 = time.perf_counter()

        artist = request.args.get("artist", "").strip()
        song = request.args.get("song", "").strip()
        country = request.args.get("country", "US").strip().upper()
        timestamps = (
            request.args.get("timestamps", "false").lower() == "true"
            or request.args.get("timestamp", "false").lower() == "true"
        )
        pass_param = request.args.get("pass", "false").lower() == "true"
        sequence = request.args.get("sequence", None)
        fast_mode = request.args.get("fast", "false").lower() == "true"
        analyze_mood = request.args.get("mood", "false").lower() == "true"
        include_metadata = request.args.get("metadata", "false").lower() == "true"

        if not artist or not song:
            return jsonify({
                "status": "error",
                "error": {
                    "message": "Artist and song name are required",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            }), 400

        if pass_param and not sequence:
            return jsonify({
                "status": "error",
                "error": {
                    "message": "Sequence parameter is required when pass=true",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            }), 400

        logger.info(
            f"[perf] rid={rid} Lyrics request: {artist} - {song} "
            f"(fast={fast_mode}, ts={timestamps}, mood={analyze_mood}, metadata={include_metadata})"
        )

        # Record user query for analytics
        try:
            trending_engine.record_user_query(
                user_id=request.remote_addr,
                query=f"{artist} - {song}",
                country=country
            )
        except Exception as e:
            logger.warning(f"Failed to record user query: {str(e)}")

        cache_key = make_cache_key(artist, song, timestamps, sequence, fast_mode, analyze_mood, include_metadata)

        # Start metadata in background (runs while lyrics fetching)
        meta_future = None
        if include_metadata:
            meta_future = _META_BG_EXEC.submit(get_metadata_only, artist, song)

        async def _resolve_stream():
            try:
                songs = await search_jiosaavn(f"{artist} {song}", limit=1)
                if songs and songs[0].get("perma_url"):
                    return await get_jiosaavn_stream(songs[0]["perma_url"])
            except Exception:
                return None
            return None

        def _should_cache(res: dict) -> bool:
            """
            Cache:
            - success with lyrics/plain
            - partial with stream_url (helps stability)
            Do NOT cache plain error with no stream.
            """
            if not isinstance(res, dict):
                return False

            st = res.get("status")
            if st == "success":
                data = res.get("data", {}) or {}
                return bool(data.get("lyrics") or data.get("plain_lyrics") or data.get("lyrics_text") or data.get("timed_lyrics"))

            if st == "partial":
                meta = res.get("metadata") or {}
                if isinstance(meta, dict) and (meta.get("stream_url") or meta.get("playable_url")):
                    return True

            return False

        def _compute():
            nonlocal meta_future

            # Fetch lyrics + stream in parallel
            t_lyrics0 = time.perf_counter()

            async def _lyrics_and_stream():
                lyrics_task = asyncio.create_task(fetch_lyrics_controller(
                    artist,
                    song,
                    timestamps=timestamps,
                    pass_param=pass_param,
                    sequence=sequence,
                    fast_mode=fast_mode,
                ))
                stream_task = asyncio.create_task(_resolve_stream())

                lyrics_res = await lyrics_task

                try:
                    stream_res = await asyncio.wait_for(stream_task, timeout=8.0)
                except Exception:
                    stream_res = None

                return lyrics_res, stream_res

            try:
                result, stream_res = run_async(_lyrics_and_stream(), timeout=60)
            except Exception as e:
                logger.error(f"[perf] rid={rid} lyrics_fetch_error={e}")
                return {
                    "status": "error",
                    "error": {
                        "message": "Failed to fetch lyrics",
                        "details": str(e),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                }

            t_lyrics = time.perf_counter() - t_lyrics0

            if not isinstance(result, dict):
                return {
                    "status": "error",
                    "error": {
                        "message": "Invalid response from lyrics fetcher",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                }

            # Always attach stream URL if available (even if lyrics failed)
            if stream_res and isinstance(stream_res, dict) and stream_res.get("stream_url"):
                result.setdefault("metadata", {})
                result["metadata"]["stream_url"] = stream_res["stream_url"]
                result["metadata"]["playable_url"] = stream_res["stream_url"]

            # Mood analysis (CPU) — only if lyrics success
            t_mood0 = time.perf_counter()
            if analyze_mood and result.get("status") == "success":
                data = result.get("data", {}) or {}
                lyrics_text = extract_lyrics_text(data)
                if lyrics_text:
                    try:
                        sentiment = analyze_sentiment(lyrics_text)
                        word_freq = analyze_word_frequency(lyrics_text, top_n=5)
                        result["mood_analysis"] = {"sentiment": sentiment, "top_words": word_freq}
                    except Exception as e:
                        result["mood_analysis"] = {"error": "Unable to perform mood analysis", "details": str(e)}
                else:
                    result["mood_analysis"] = {"error": "Unable to extract lyrics for analysis"}
            t_mood = time.perf_counter() - t_mood0

            # Metadata (best-effort; do NOT block long)
            t_meta0 = time.perf_counter()
            if include_metadata and result.get("status") in ("success", "partial"):
                try:
                    meta_payload = None
                    if meta_future:
                        try:
                            meta_payload = meta_future.result(timeout=META_BUDGET_SECONDS)
                        except FuturesTimeout:
                            meta_payload = None
                        except Exception:
                            meta_payload = None

                    if meta_payload and isinstance(meta_payload, dict) and meta_payload.get("status") == "success":
                        # merge metadata but don't destroy stream_url already set above
                        result.setdefault("metadata", {})
                        for k, v in (meta_payload.get("metadata") or {}).items():
                            if k in ("stream_url", "playable_url"):
                                continue
                            result["metadata"][k] = v
                    else:
                        result.setdefault("metadata", {})
                        result["metadata_error"] = "Metadata unavailable or timed out"
                except Exception as e:
                    result.setdefault("metadata", {})
                    result["metadata_error"] = f"Could not retrieve metadata: {str(e)}"
            t_meta = time.perf_counter() - t_meta0

            total = time.perf_counter() - t0
            logger.info(
                f"[perf] rid={rid} total={total:.3f}s lyrics={t_lyrics:.3f}s mood={t_mood:.3f}s meta={t_meta:.3f}s "
                f"fast={fast_mode} ts={timestamps} moodOn={analyze_mood} metaOn={include_metadata}"
            )

            # If lyrics failed but stream exists -> make it partial
            if result.get("status") == "error":
                meta = result.get("metadata") or {}
                stream_url = meta.get("stream_url") or meta.get("playable_url") if isinstance(meta, dict) else None
                if stream_url:
                    result["status"] = "partial"

            return result

        # Coalesced cache fetch (prevents thundering herd)
        result, hit = get_or_fetch_coalesced(cache_key, _compute, should_cache=_should_cache)

        if hit:
            total = time.perf_counter() - t0
            logger.info(f"[perf] rid={rid} cache_hit total={total:.3f}s")

        # Proper HTTP semantics:
        # - success / partial -> 200
        # - error without stream -> 404
        if isinstance(result, dict) and result.get("status") == "error":
            return jsonify(result), 404
        return jsonify(result), 200

    @app.route("/metadata/", methods=["GET"])
    def metadata():
        artist = request.args.get("artist", "").strip()
        song = request.args.get("song", "").strip()
        if not artist or not song:
            return jsonify({
                "status": "error",
                "error": {"message": "Artist and song name are required", "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 400

        try:
            result = get_metadata_only(artist, song)
            if asyncio.iscoroutine(result):
                result = run_async(result, timeout=30)
            return jsonify(result), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to fetch metadata", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

    @app.route("/trending/", methods=["GET"])
    def trending():
        country = request.args.get("country", "US").strip().upper()
        countries_param = request.args.get("countries", "").strip()
        limit = request.args.get("limit", 20, type=int)
        if limit < 1 or limit > 100:
            limit = 20

        try:
            if country and not countries_param:
                try:
                    country_enum = Country[country]
                except KeyError:
                    return jsonify({
                        "status": "error",
                        "error": {"message": f"Invalid country code: {country}", "timestamp": datetime.now(timezone.utc).isoformat()},
                    }), 400

                trending_songs = trending_engine.fetch_trending_songs(country_enum, limit)
                return jsonify({
                    "status": "success",
                    "data": {
                        "country": country,
                        "trending": [song.to_dict() for song in trending_songs],
                        "total": len(trending_songs),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                }), 200

            if countries_param:
                country_list = [c.strip().upper() for c in countries_param.split(",")]
                trending_data = {}
                for c in country_list:
                    try:
                        country_enum = Country[c]
                        trending_songs = trending_engine.fetch_trending_songs(country_enum, limit)
                        trending_data[c] = [song.to_dict() for song in trending_songs]
                    except KeyError:
                        continue

                return jsonify({
                    "status": "success",
                    "data": {"countries": trending_data, "timestamp": datetime.now(timezone.utc).isoformat()}
                }), 200

            return jsonify({
                "status": "success",
                "data": {"countries": {}, "timestamp": datetime.now(timezone.utc).isoformat()}
            }), 200

        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to fetch trending data", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
            }), 500

    @app.route("/api/jiosaavn/search", methods=["GET"])
    def jiosaavn_search():
        query = request.args.get("q", "").strip()
        if not query:
            return jsonify({
                "status": "error",
                "error": {"message": "Query parameter 'q' is required", "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 400

        try:
            results = search_jiosaavn(query)
            if asyncio.iscoroutine(results):
                results = run_async(results, timeout=30)
            return jsonify({"status": "success", "results": results}), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to search JioSaavn", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

    @app.route("/api/jiosaavn/play", methods=["GET"])
    def jiosaavn_play():
        song_link = request.args.get("songLink", "").strip()
        if not song_link:
            return jsonify({
                "status": "error",
                "error": {"message": "songLink parameter is required", "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 400

        try:
            data = get_jiosaavn_stream(song_link)
            if asyncio.iscoroutine(data):
                data = run_async(data, timeout=30)
            return jsonify({"status": "success", "data": data}), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to fetch stream", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

    @app.route("/suggestion", methods=["GET"])
    def suggestion():
        query = request.args.get("q", "").strip()
        limit = request.args.get("limit", 10, type=int)
        if not query:
            return jsonify({
                "status": "error",
                "error": {"message": "Query parameter 'q' is required", "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 400
        if limit < 1 or limit > 100:
            limit = 10

        try:
            with _httpx.Client(timeout=10) as client:
                resp = client.get(
                    "https://musicbrainz.org/ws/2/recording/",
                    params={"query": query, "fmt": "json", "limit": limit},
                    headers={"User-Agent": "Lyrica/1.0 (https://github.com/Wilooper/Lyrica)"},
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to fetch suggestions from MusicBrainz", "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

        recordings = data.get("recordings", [])
        results = []
        for rec in recordings:
            title = rec.get("title", "")
            artist_credits = rec.get("artist-credit", [])
            artist_parts = []
            for credit in artist_credits:
                if isinstance(credit, dict) and "artist" in credit:
                    artist_parts.append(credit["artist"].get("name", ""))
                    artist_parts.append(credit.get("joinphrase", ""))
                elif isinstance(credit, str):
                    artist_parts.append(credit)
            artist_name = "".join(artist_parts).strip() or "Unknown Artist"
            results.append({"title": title, "artist": artist_name})

        return jsonify({"status": "success", "query": query, "limit": limit, "total": len(results), "results": results}), 200

    @app.route("/app", methods=["GET"])
    def app_page():
        try:
            return render_template("index.html")
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to load application", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

    @app.route("/cache/stats", methods=["GET"])
    def route_cache_stats():
        try:
            stats = cache_stats()
            return jsonify({"status": "success", **stats}), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to retrieve cache stats", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

    @app.route("/cache/clear", methods=["POST"])
    def route_clear_cache():
        key = request.args.get("key") or request.headers.get("X-ADMIN-KEY")
        if not ADMIN_KEY or key != ADMIN_KEY:
            return jsonify({"status": "error", "error": {"message": "Unauthorized"}}), 403
        try:
            res = clear_cache()
            return jsonify({"status": "success", "details": res}), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": {"message": "Failed to clear cache", "details": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
            }), 500

    @app.route("/favicon.ico", methods=["GET"])
    def favicon():
        return "", 204

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({
            "status": "error",
            "error": {"message": "Endpoint not found", "timestamp": datetime.now(timezone.utc).isoformat()},
        }), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {str(error)}")
        return jsonify({
            "status": "error",
            "error": {"message": "Internal server error", "timestamp": datetime.now(timezone.utc).isoformat()},
        }), 500
