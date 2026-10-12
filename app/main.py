"""HTTP API of one stateless cart-service instance (run N of them behind a load balancer)."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import time
import uuid

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from . import config
from .auth import AuthError, issue_token, require
from .logging_setup import event, setup
from .metrics import METRICS
from .redis_client import Redis, RedisError
from .store import (EVENTS_STREAM, ConsistentCartStore, L1Cache, NaiveCartStore,
                    OrderStore, ProductStore, StoreError, UserStore, cart_key)

log = setup()
redis = Redis(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
cache = L1Cache(config.L1_CACHE_TTL_SECONDS, config.L1_CACHE_ENABLED)
users = UserStore(redis)
products = ProductStore(redis)
orders = OrderStore(redis)
carts = (NaiveCartStore if config.CONSISTENCY_MODE == "naive" else ConsistentCartStore)(redis, cache)
chaos: dict = {"mode": "off"}
STARTED = time.time()

SKU_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
IDEM_RE = re.compile(r"^[A-Za-z0-9_.:-]{8,100}$")


# ---------------------------------------------------------------- helpers
class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, data=None):
        self.status, self.code, self.message, self.data = status, code, message, data


async def body(request: Request) -> dict:
    try:
        data = await request.json()
    except Exception:
        raise ApiError(400, "BAD_JSON", "request body must be JSON")
    if not isinstance(data, dict):
        raise ApiError(400, "BAD_JSON", "request body must be a JSON object")
    return data


def int_field(data: dict, name: str, lo: int, hi: int) -> int:
    v = data.get(name)
    if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
        raise ApiError(422, "VALIDATION", f"'{name}' must be an integer in [{lo}, {hi}]")
    return v


def if_match(request: Request, required: bool) -> int | None:
    h = request.headers.get("if-match")
    if not h:
        if required:
            raise ApiError(428, "PRECONDITION_REQUIRED",
                           "send If-Match: \"<cart version>\" (from the ETag of GET /cart)")
        return None
    m = re.fullmatch(r'(?:W/)?"?v?(\d+)"?', h.strip())
    if not m:
        raise ApiError(400, "BAD_IF_MATCH", "If-Match must look like \"7\"")
    return int(m.group(1))


def idem_header(request: Request) -> str:
    k = request.headers.get("idempotency-key", "")
    if k and not IDEM_RE.fullmatch(k):
        raise ApiError(400, "BAD_IDEMPOTENCY_KEY", "Idempotency-Key: 8-100 chars [A-Za-z0-9_.:-]")
    return k


async def render_cart(state: dict) -> dict:
    cat = await products.all()
    lines, total = [], 0
    for sku, qty in sorted(state["items"].items()):
        p = cat.get(sku, {"name": "?", "price_cents": 0})
        lines.append({"sku": sku, "name": p["name"], "qty": qty,
                      "unit_price_cents": p["price_cents"], "line_total_cents": qty * p["price_cents"]})
        total += qty * p["price_cents"]
    return {"version": state["version"], "items": lines,
            "item_count": sum(state["items"].values()), "total_cents": total,
            "updated_at": state.get("updated_at")}


def cart_response(rendered: dict, status: int = 200, replay: bool = False) -> JSONResponse:
    headers = {"ETag": f'"{rendered["version"]}"'}
    if replay:
        headers["Idempotent-Replayed"] = "true"
    return JSONResponse(rendered, status_code=status, headers=headers)


async def maybe_crash_after_commit(where: str) -> None:
    """Controlled failure: the write is committed in Redis but this instance dies
    before answering. The load balancer retries on another instance; the
    idempotency key turns that retry into a harmless replay."""
    if chaos.get("mode") == "crash_after_commit" and random.random() < chaos.get("probability", 1.0):
        event(log, "chaos_crash_after_commit", logging.WARNING, where=where)
        await redis.execute("HINCRBY", "metrics:cluster", "chaos_crashes", 1)
        os._exit(17)


async def cluster_inc(name: str) -> None:
    with contextlib.suppress(Exception):
        await redis.execute("HINCRBY", "metrics:cluster", name, 1)


# ---------------------------------------------------------------- public
async def health(request: Request):
    try:
        ok = await asyncio.wait_for(redis.ping(), 1)
    except Exception:
        ok = False
    body_ = {"status": "ok" if ok else "degraded", "instance": config.INSTANCE_ID,
             "mode": carts.mode, "redis": ok, "uptime_s": round(time.time() - STARTED, 1)}
    return JSONResponse(body_, status_code=200 if ok else 503)


async def metrics(request: Request):
    return PlainTextResponse(METRICS.render(config.INSTANCE_ID),
                             media_type="text/plain; version=0.0.4")


async def register(request: Request):
    d = await body(request)
    name, pw = d.get("username"), d.get("password")
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{3,32}", name):
        raise ApiError(422, "VALIDATION", "username: 3-32 chars [A-Za-z0-9_.-]")
    if not isinstance(pw, str) or len(pw) < 8:
        raise ApiError(422, "VALIDATION", "password must be at least 8 characters")
    u = await users.create(name, pw)
    event(log, "user_registered", user=u["id"], username=name)
    return JSONResponse({**u, "token": issue_token(u["id"], u["username"], u["role"])}, 201)


async def login(request: Request):
    d = await body(request)
    u = await users.authenticate(str(d.get("username", "")), str(d.get("password", "")))
    if not u:
        event(log, "login_failed", logging.WARNING, username=str(d.get("username", ""))[:40])
        raise ApiError(401, "BAD_CREDENTIALS", "wrong username or password")
    event(log, "login_ok", user=u["id"], role=u["role"])
    return JSONResponse({**u, "token": issue_token(u["id"], u["username"], u["role"]),
                         "expires_in": config.JWT_TTL_SECONDS})


async def list_products(request: Request):
    return JSONResponse({"products": sorted((await products.all()).values(), key=lambda p: p["sku"])})


# ---------------------------------------------------------------- cart
async def get_cart(request: Request):
    p = require(request, "customer", "admin")
    state = await carts.get(p["sub"])
    return cart_response(await render_cart(state))


async def add_item(request: Request):
    p = require(request, "customer")
    d = await body(request)
    sku = d.get("sku")
    if not isinstance(sku, str) or not SKU_RE.fullmatch(sku):
        raise ApiError(422, "VALIDATION", "'sku' is required")
    qty = int_field(d, "qty", 1, 1000)
    state, replay = await carts.mutate(p["sub"], "add", sku, qty, if_match(request, False),
                                       idem_header(request))
    event(log, "cart_item_added", user=p["sub"], sku=sku, qty=qty, version=state["version"],
          replay=replay, idem=request.headers.get("idempotency-key"))
    await maybe_crash_after_commit("add_item")
    return cart_response(await render_cart(state), 200, replay)


async def set_item(request: Request):
    p = require(request, "customer")
    sku = request.path_params["sku"]
    qty = int_field(await body(request), "qty", 0, 1000)
    # absolute writes overwrite, so they MUST be based on the latest version
    expected = if_match(request, carts.mode == "consistent")
    state, replay = await carts.mutate(p["sub"], "set", sku, qty, expected, idem_header(request))
    event(log, "cart_item_set", user=p["sub"], sku=sku, qty=qty, version=state["version"], replay=replay)
    return cart_response(await render_cart(state), 200, replay)


async def remove_item(request: Request):
    p = require(request, "customer")
    sku = request.path_params["sku"]
    state, replay = await carts.mutate(p["sub"], "remove", sku, 0, if_match(request, False),
                                       idem_header(request))
    event(log, "cart_item_removed", user=p["sub"], sku=sku, version=state["version"])
    return cart_response(await render_cart(state), 200, replay)


async def clear_cart(request: Request):
    p = require(request, "customer")
    state, replay = await carts.mutate(p["sub"], "clear", "", 0, if_match(request, False),
                                       idem_header(request))
    event(log, "cart_cleared", user=p["sub"], version=state["version"])
    return cart_response(await render_cart(state), 200, replay)


async def checkout(request: Request):
    p = require(request, "customer")
    expected = if_match(request, carts.mode == "consistent")
    res, replay = await carts.checkout(p["sub"], expected if expected is not None else -1,
                                       idem_header(request))
    event(log, "checkout_ok", user=p["sub"], order=res["order_id"], total_cents=res["total_cents"],
          replay=replay)
    await maybe_crash_after_commit("checkout")
    headers = {"Location": f"/api/v1/orders/{res['order_id']}"}
    if replay:
        headers["Idempotent-Replayed"] = "true"
    return JSONResponse(res, status_code=201, headers=headers)


async def list_orders(request: Request):
    p = require(request, "customer", "admin")
    return JSONResponse({"orders": await orders.list_for(p["sub"])})


async def get_order(request: Request):
    p = require(request, "customer", "admin")
    o = await orders.get(request.path_params["order_id"])
    if not o or (p["role"] != "admin" and o["user"] != p["sub"]):
        raise ApiError(404, "ORDER_NOT_FOUND", "no such order")
    return JSONResponse(o)


# ---------------------------------------------------------------- admin
async def upsert_product(request: Request):
    p = require(request, "admin")
    d = await body(request)
    sku = d.get("sku")
    if not isinstance(sku, str) or not SKU_RE.fullmatch(sku):
        raise ApiError(422, "VALIDATION", "'sku' is required")
    name = d.get("name")
    if not isinstance(name, str) or not 1 <= len(name) <= 120:
        raise ApiError(422, "VALIDATION", "'name' is required")
    prod = await products.upsert(sku, name, int_field(d, "price_cents", 0, 10**9),
                                 int_field(d, "stock", 0, 10**9))
    event(log, "product_upserted", admin=p["sub"], sku=sku)
    return JSONResponse(prod, 201)


async def cluster(request: Request):
    require(request, "admin", "service")
    keys, cursor = [], "0"
    while True:
        cursor, batch = await redis.execute("SCAN", cursor, "MATCH", "instance:*", "COUNT", 100)
        keys += batch
        if cursor == "0":
            break
    inst = [json.loads(v) for v in (await redis.pipeline([("GET", k) for k in keys]) if keys else []) if v]
    flat = await redis.execute("HGETALL", "metrics:cluster") or []
    info = await redis.execute("INFO", "replication")
    role = re.search(r"role:(\w+)", info)
    replicas = re.search(r"connected_slaves:(\d+)", info)
    return JSONResponse({"live_instances": sorted(inst, key=lambda i: i["id"]),
                         "cluster_counters": {k: int(v) for k, v in zip(flat[::2], flat[1::2])},
                         "redis": {"role": role.group(1) if role else "?",
                                   "connected_replicas": int(replicas.group(1)) if replicas else 0,
                                   "events_in_stream": await redis.execute("XLEN", EVENTS_STREAM)}})


async def recent_events(request: Request):
    require(request, "admin", "service")
    n = min(int(request.query_params.get("count", "20")), 500)
    user = request.query_params.get("user")
    raw = await redis.execute("XREVRANGE", EVENTS_STREAM, "+", "-", "COUNT", n if not user else 5000)
    out = []
    for eid, flat in raw or []:
        d = dict(zip(flat[::2], flat[1::2]))
        if user and d.get("user") != user:
            continue
        out.append({"id": eid, **d})
        if len(out) >= n:
            break
    return JSONResponse({"events": out})


async def raw_cart(request: Request):
    require(request, "admin", "service")
    uid = request.path_params["user_id"]
    k = cart_key(uid)
    flat = await redis.execute("HGETALL", k) or []
    ttl = await redis.execute("TTL", k)
    return JSONResponse({"key": k, "ttl_seconds": ttl, "hash": dict(zip(flat[::2], flat[1::2]))})


async def set_chaos(request: Request):
    p = require(request, "admin")
    d = await body(request)
    mode = d.get("mode", "off")
    if mode not in ("off", "crash_after_commit", "latency", "crash_now"):
        raise ApiError(422, "VALIDATION", "mode: off | crash_after_commit | latency | crash_now")
    target = d.get("instance", "all")
    cfg = {"mode": mode, "probability": float(d.get("probability", 1.0)),
           "latency_ms": int(d.get("latency_ms", 0)), "set_by": p["sub"]}
    ttl = int(d.get("ttl_seconds", 120))
    await redis.execute("SET", f"chaos:{target}", json.dumps(cfg), "EX", ttl)
    event(log, "chaos_configured", logging.WARNING, target=target, **cfg)
    return JSONResponse({"target": target, "ttl_seconds": ttl, **cfg})


# ---------------------------------------------------------------- middleware
async def app_middleware(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    t0 = time.perf_counter()
    route = request.scope.get("route")
    path_tpl = getattr(route, "path", request.url.path)
    if chaos.get("mode") == "latency":
        await asyncio.sleep(chaos.get("latency_ms", 0) / 1000)
    try:
        resp: Response = await call_next(request)
    except (ApiError, StoreError, AuthError) as e:
        resp = JSONResponse({"error": e.code, "message": e.message,
                             **({"current": await render_cart(e.data)} if getattr(e, "data", None) else {})},
                            status_code=e.status)
        if getattr(e, "data", None):
            resp.headers["ETag"] = f'"{e.data["version"]}"'
        if e.status == 409 and e.code == "VERSION_CONFLICT":
            await cluster_inc("version_conflicts")
            event(log, "version_conflict", logging.WARNING, path=request.url.path, request_id=rid)
    except (ConnectionError, OSError, asyncio.TimeoutError, RedisError) as e:
        event(log, "storage_unavailable", logging.ERROR, error=str(e), request_id=rid)
        METRICS.inc("storage_errors_total")
        resp = JSONResponse({"error": "STORAGE_UNAVAILABLE", "message": "try again"}, 503,
                            headers={"Retry-After": "1"})
    except Exception as e:  # pragma: no cover
        log.exception("unhandled_error", extra={"fields": {"request_id": rid}})
        resp = JSONResponse({"error": "INTERNAL", "message": str(e)}, 500)
    dt = time.perf_counter() - t0
    route = request.scope.get("route")
    path_tpl = getattr(route, "path", path_tpl)
    METRICS.inc("cart_http_requests_total", method=request.method, route=path_tpl,
                status=str(resp.status_code))
    METRICS.observe(f"{request.method} {path_tpl}", dt)
    resp.headers["X-Instance"] = config.INSTANCE_ID
    resp.headers["X-Request-Id"] = rid
    resp.headers["X-Consistency-Mode"] = carts.mode
    if path_tpl not in ("/health", "/metrics"):
        event(log, "http_request", method=request.method, path=request.url.path,
              status=resp.status_code, ms=round(dt * 1000, 2), request_id=rid,
              replay=resp.headers.get("Idempotent-Replayed") == "true")
    return resp


# ---------------------------------------------------------------- background
async def heartbeat_loop():
    while True:
        try:
            await redis.execute("SET", f"instance:{config.INSTANCE_ID}", json.dumps({
                "id": config.INSTANCE_ID, "mode": carts.mode, "pid": os.getpid(),
                "started_at": int(STARTED), "last_seen": int(time.time())}),
                "EX", int(config.HEARTBEAT_SECONDS * 3))
            raw = (await redis.execute("GET", f"chaos:{config.INSTANCE_ID}")
                   or await redis.execute("GET", "chaos:all"))
            new = json.loads(raw) if raw else {"mode": "off"}
            if new != chaos:
                chaos.clear(); chaos.update(new)
                event(log, "chaos_mode_changed", logging.WARNING, **new)
                if new.get("mode") == "crash_now":
                    await redis.execute("DEL", f"chaos:{config.INSTANCE_ID}")
                    event(log, "chaos_crash_now", logging.WARNING)
                    os._exit(17)
        except Exception as e:
            event(log, "heartbeat_failed", logging.ERROR, error=str(e))
        await asyncio.sleep(config.HEARTBEAT_SECONDS)


def on_invalidate(msg: str) -> None:
    uid, _, ver = msg.rpartition(":")
    cache.invalidate(uid, int(ver))


def on_resubscribe() -> None:
    cache.clear()  # messages may have been missed while disconnected
    event(log, "invalidation_channel_subscribed")


@contextlib.asynccontextmanager
async def lifespan(app):
    for attempt in range(30):
        try:
            await redis.ping()
            break
        except Exception as e:
            event(log, "redis_wait", logging.WARNING, attempt=attempt, error=str(e))
            await asyncio.sleep(1)
    tasks = [asyncio.create_task(heartbeat_loop())]
    if carts.mode == "consistent":
        tasks.append(asyncio.create_task(redis.subscribe_forever("cart-invalidate", on_invalidate,
                                                                 on_resubscribe)))
    tasks.append(asyncio.create_task(redis.subscribe_forever(
        "product-invalidate", lambda _m: products.invalidate())))
    event(log, "instance_started", mode=carts.mode, l1_cache=config.L1_CACHE_ENABLED, pid=os.getpid())
    yield
    for t in tasks:
        t.cancel()
    with contextlib.suppress(Exception):
        await redis.execute("DEL", f"instance:{config.INSTANCE_ID}")
    event(log, "instance_stopped")


V = "/api/v1"
routes = [
    Route("/health", health),
    Route("/metrics", metrics),
    Route(f"{V}/auth/register", register, methods=["POST"]),
    Route(f"{V}/auth/login", login, methods=["POST"]),
    Route(f"{V}/products", list_products, methods=["GET"]),
    Route(f"{V}/products", upsert_product, methods=["POST"]),
    Route(f"{V}/cart", get_cart, methods=["GET"]),
    Route(f"{V}/cart", clear_cart, methods=["DELETE"]),
    Route(f"{V}/cart/items", add_item, methods=["POST"]),
    Route(f"{V}/cart/items/{{sku}}", set_item, methods=["PUT"]),
    Route(f"{V}/cart/items/{{sku}}", remove_item, methods=["DELETE"]),
    Route(f"{V}/cart/checkout", checkout, methods=["POST"]),
    Route(f"{V}/orders", list_orders, methods=["GET"]),
    Route(f"{V}/orders/{{order_id}}", get_order, methods=["GET"]),
    Route(f"{V}/admin/cluster", cluster, methods=["GET"]),
    Route(f"{V}/admin/events", recent_events, methods=["GET"]),
    Route(f"{V}/admin/carts/{{user_id}}", raw_cart, methods=["GET"]),
    Route(f"{V}/admin/chaos", set_chaos, methods=["POST"]),
]

app = Starlette(routes=routes, lifespan=lifespan,
                middleware=[Middleware(BaseHTTPMiddleware, dispatch=app_middleware)])
