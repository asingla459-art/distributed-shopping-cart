"""Runtime configuration, read once from environment variables."""
import os
import socket
import uuid


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


REDIS_HOST = _env("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(_env("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None

# Unique id of this application instance (shown in every log line / response header)
INSTANCE_ID = _env("INSTANCE_ID", f"{socket.gethostname()}-{uuid.uuid4().hex[:4]}")

# "consistent" = versioned atomic Lua updates + idempotency + cache invalidation
# "naive"      = read-modify-write in the app, no versions, no idempotency,
#                per-instance cache without invalidation (the "before" baseline)
CONSISTENCY_MODE = _env("CONSISTENCY_MODE", "consistent")

JWT_SECRET = _env("JWT_SECRET", "dev-only-change-me-please-32-bytes!!")
JWT_TTL_SECONDS = int(_env("JWT_TTL_SECONDS", "3600"))

CART_TTL_SECONDS = int(_env("CART_TTL_SECONDS", str(7 * 24 * 3600)))   # idle cart expiry
IDEMPOTENCY_TTL_SECONDS = int(_env("IDEMPOTENCY_TTL_SECONDS", str(24 * 3600)))
L1_CACHE_TTL_SECONDS = float(_env("L1_CACHE_TTL_SECONDS", "30"))
L1_CACHE_ENABLED = _env("L1_CACHE_ENABLED", "1") == "1"
HEARTBEAT_SECONDS = float(_env("HEARTBEAT_SECONDS", "2"))
MAX_QTY_PER_ITEM = int(_env("MAX_QTY_PER_ITEM", "1000000"))

LOG_FILE = os.environ.get("LOG_FILE")  # optional extra JSON-lines file
