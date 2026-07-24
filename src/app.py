from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_compress import Compress
from src.logger import get_logger
from src import __version__
from src.router import register_routes
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import os

# Admin cache endpoints
from src.cache import clear_cache, cache_stats
from src.config import ADMIN_KEY


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean env var. Accepts: 1/0, true/false, yes/no, on/off."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "enabled"}


def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    CORS(
        app,
        resources={
            r"/*": {
                "origins": "*",
                "allow_headers": ["Content-Type"],
                "expose_headers": ["Access-Control-Allow-Origin"],
            }
        },
    )

    # Gzip compress all responses — reduces payload size by 60-80%
    Compress(app)

    app.logger = get_logger("Lyrica")
    app.config["VERSION"] = __version__

    # ------------------------------------------------------------------
    # Rate limiting — env-driven for personal deployments.
    #
    # Env vars:
    #   RATELIMIT_ENABLED   default "true"
    #                       set to "false" to disable rate limiting entirely
    #                       (recommended for single-user personal deployments)
    #   RATELIMIT_DEFAULT   default "15 per minute"
    #                       any Flask-Limiter limit string, e.g.
    #                       "300 per minute" or "60 per second"
    #   RATELIMIT_RETRY_AFTER  default "35"
    #                       seconds the 429 handler advertises via Retry-After
    #   RATE_LIMIT_STORAGE_URI default "memory://"
    #                       use Redis for multi-worker deployments
    # ------------------------------------------------------------------
    ratelimit_enabled = _env_bool("RATELIMIT_ENABLED", True)
    default_limit_str = os.getenv("RATELIMIT_DEFAULT", "15 per minute")
    retry_after_seconds = os.getenv("RATELIMIT_RETRY_AFTER", "35")
    storage_uri = os.getenv("RATE_LIMIT_STORAGE_URI", "memory://")

    limiter = Limiter(
        key_func=get_remote_address,
        storage_uri=storage_uri,
        headers_enabled=True,
        default_limits=[default_limit_str] if ratelimit_enabled else [],
        enabled=ratelimit_enabled,
    )
    limiter.init_app(app)

    app.logger.info(
        "rate_limit config: enabled=%s default=%s retry_after=%ss storage=%s",
        ratelimit_enabled,
        default_limit_str if ratelimit_enabled else "N/A",
        retry_after_seconds,
        storage_uri,
    )

    # NEW: Admin helper function
    def admin_required(req):
        key = req.args.get("key") or req.headers.get("X-ADMIN-KEY")
        return key == ADMIN_KEY

    # Custom 429 error handler
    @app.errorhandler(429)
    def ratelimit_handler(e):
        resp = jsonify(
            {
                "status": "error",
                "error": {
                    "message": f"Rate limit exceeded. Please wait {retry_after_seconds} seconds before retrying.",
                },
            }
        )
        resp.status_code = 429
        resp.headers["Retry-After"] = str(retry_after_seconds)
        return resp

    # Secure admin endpoints
    @app.route("/admin/cache/clear", methods=["GET"])
    def admin_clear_cache():
        if not admin_required(request):
            return {"error": "unauthorized"}, 403
        result = clear_cache()
        return {"status": "cache cleared", "details": result}

    @app.route("/admin/cache/stats", methods=["GET"])
    def admin_cache_stats():
        if not admin_required(request):
            return {"error": "unauthorized"}, 403
        return cache_stats()

    register_routes(app)
    return app
