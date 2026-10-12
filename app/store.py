"""Data access layer: users, products, carts, orders.

Two cart strategies share one interface so the experiments can compare them:

* ConsistentCartStore  - every write is one atomic Lua script with version
  checks (optimistic concurrency), idempotency keys, sliding TTL, an audit
  event and a pub/sub invalidation for the other instances' L1 caches.
* NaiveCartStore       - the typical "load cart object, modify, save it back"
  design with an instance-local cache and no versions/idempotency.
  It is kept ONLY as the 'before' baseline for the experiments.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from . import config
from .metrics import METRICS
from .redis_client import Redis

LUA_DIR = Path(__file__).parent / "lua"
EVENTS_STREAM = "stream:cart-events"
ORDERS_STREAM = "stream:orders"


def cart_key(user_id: str) -> str:
    # {user_id} is a Redis Cluster hash tag: cart + idempotency keys of one user
    # always land on the same shard, so the Lua script stays single-slot.
    return f"cart:{{{user_id}}}"


def idem_key(user_id: str, key: str) -> str:
    return f"idem:{{{user_id}}}:{key}" if key else ""


def now_ms() -> int:
    return int(time.time() * 1000)


def fingerprint(*parts) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]


class StoreError(Exception):
    def __init__(self, status: int, code: str, message: str, data=None):
        super().__init__(message)
        self.status, self.code, self.message, self.data = status, code, message, data


# --------------------------------------------------------------------------
# Users & products
# --------------------------------------------------------------------------
def hash_password(password: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, salt_hex, dk_hex = stored.split("$")
    except ValueError:
        return False
    cand = hash_password(password, bytes.fromhex(salt_hex))
    return secrets.compare_digest(cand, stored)


class UserStore:
    def __init__(self, r: Redis):
        self.r = r

    async def create(self, username: str, password: str, role: str = "customer") -> dict:
        uid = "u_" + uuid.uuid4().hex[:12]
        # HSETNX on the username index makes registration race-free (no duplicate users)
        ok = await self.r.execute("HSETNX", "users:by_name", username, uid)
        if ok == 0:
            raise StoreError(409, "USERNAME_TAKEN", "username already exists")
        await self.r.execute("HSET", f"user:{uid}", "id", uid, "username", username,
                             "password_hash", await asyncio.to_thread(hash_password, password),
                             "role", role, "created_at", now_ms())
        return {"id": uid, "username": username, "role": role}

    async def authenticate(self, username: str, password: str) -> dict | None:
        uid = await self.r.execute("HGET", "users:by_name", username)
        if not uid:
            return None
        flat = await self.r.execute("HGETALL", f"user:{uid}")
        u = dict(zip(flat[::2], flat[1::2]))
        if not await asyncio.to_thread(verify_password, password, u.get("password_hash", "")):
            return None
        return {"id": u["id"], "username": u["username"], "role": u["role"]}


class ProductStore:
    """Product catalogue. Read-mostly, so cached per instance for 60 s."""

    def __init__(self, r: Redis):
        self.r = r
        self._cache: dict[str, dict] = {}
        self._cache_at = 0.0

    async def upsert(self, sku: str, name: str, price_cents: int, stock: int) -> dict:
        await self.r.pipeline([
            ("HSET", f"product:{sku}", "sku", sku, "name", name,
             "price_cents", price_cents, "stock", stock),
            ("SADD", "products", sku),
            ("PUBLISH", "product-invalidate", sku),
        ])
        self._cache_at = 0
        return {"sku": sku, "name": name, "price_cents": price_cents, "stock": stock}

    async def all(self, fresh: bool = False) -> dict[str, dict]:
        if not fresh and self._cache and time.time() - self._cache_at < 60:
            return self._cache
        skus = await self.r.execute("SMEMBERS", "products") or []
        res = await self.r.pipeline([("HGETALL", f"product:{s}") for s in skus]) if skus else []
        out = {}
        for flat in res:
            d = dict(zip(flat[::2], flat[1::2]))
            if d:
                out[d["sku"]] = {"sku": d["sku"], "name": d["name"],
                                 "price_cents": int(d["price_cents"]), "stock": int(d["stock"])}
        self._cache, self._cache_at = out, time.time()
        return out

    def invalidate(self) -> None:
        self._cache_at = 0


# --------------------------------------------------------------------------
# Instance-local (L1) cart cache
# --------------------------------------------------------------------------
@dataclass
class _Entry:
    state: dict
    at: float = field(default_factory=time.monotonic)


class L1Cache:
    def __init__(self, ttl: float, enabled: bool = True):
        self.ttl, self.enabled = ttl, enabled
        self._d: dict[str, _Entry] = {}

    def get(self, uid: str) -> dict | None:
        if not self.enabled:
            return None
        e = self._d.get(uid)
        if e and time.monotonic() - e.at < self.ttl:
            METRICS.inc("cart_cache_hits_total")
            return e.state
        METRICS.inc("cart_cache_misses_total")
        return None

    def put(self, uid: str, state: dict) -> None:
        if not self.enabled:
            return
        cur = self._d.get(uid)
        # never replace a newer cached version with an older one
        if cur and cur.state.get("version", 0) > state.get("version", 0):
            return
        self._d[uid] = _Entry(state)

    def invalidate(self, uid: str, version: int | None = None) -> None:
        cur = self._d.get(uid)
        if cur and (version is None or cur.state.get("version", 0) < version):
            del self._d[uid]
            METRICS.inc("cart_cache_invalidations_total")

    def clear(self) -> None:
        self._d.clear()


def _parse_cart(flat: list | None) -> dict:
    items, v, upd = {}, 0, ""
    flat = flat or []
    for f, val in zip(flat[::2], flat[1::2]):
        if f == "v":
            v = int(val)
        elif f == "updated_at":
            upd = val
        elif f.startswith("item:"):
            items[f[5:]] = int(val)
    return {"version": v, "items": items, "updated_at": upd}


# --------------------------------------------------------------------------
# Cart strategies
# --------------------------------------------------------------------------
class ConsistentCartStore:
    mode = "consistent"

    def __init__(self, r: Redis, cache: L1Cache):
        self.r, self.cache = r, cache
        r.register_script("mutate", (LUA_DIR / "cart_mutate.lua").read_text())
        r.register_script("checkout", (LUA_DIR / "checkout.lua").read_text())

    async def get(self, uid: str, min_version: int = 0) -> dict:
        """min_version is a session-consistency token: the client sends the
        highest version it has seen, so the instance never serves it an older
        cached cart (read-your-writes even if the invalidation is in flight)."""
        hit = self.cache.get(uid)
        if hit is not None and hit["version"] >= min_version:
            return hit
        if hit is not None:
            METRICS.inc("cart_cache_stale_bypass_total")
        state = _parse_cart(await self.r.execute("HGETALL", cart_key(uid)))
        self.cache.put(uid, state)
        return state

    async def mutate(self, uid: str, op: str, sku: str = "", qty: int = 0,
                     expected_version: int | None = None, idem: str = "") -> tuple[dict, bool]:
        fp = fingerprint(op, sku, qty, expected_version)
        status, payload = await self.r.run_script(
            "mutate",
            [cart_key(uid), idem_key(uid, idem), EVENTS_STREAM, f"product:{sku}" if sku else ""],
            [op, sku, qty, "" if expected_version is None else expected_version,
             config.CART_TTL_SECONDS, config.IDEMPOTENCY_TTL_SECONDS, now_ms(),
             config.INSTANCE_ID, uid, fp, config.MAX_QTY_PER_ITEM])
        if status in ("OK", "REPLAY"):
            state = json.loads(payload)
            state["items"] = state["items"] or {}
            self.cache.put(uid, state)
            if status == "REPLAY":
                METRICS.inc("cart_idempotent_replays_total")
            return state, status == "REPLAY"
        if status == "CONFLICT":
            METRICS.inc("cart_version_conflicts_total")
            state = json.loads(payload)
            state["items"] = state["items"] or {}
            self.cache.put(uid, state)
            raise StoreError(409, "VERSION_CONFLICT",
                             f"cart changed (expected version {expected_version}, "
                             f"current {state['version']}); re-read and retry", state)
        raise _map_error(status, payload)

    async def checkout(self, uid: str, expected_version: int, idem: str = "") -> tuple[dict, bool]:
        state = _parse_cart(await self.r.execute("HGETALL", cart_key(uid)))
        skus = sorted(state["items"])
        order_id = "o_" + uuid.uuid4().hex[:12]
        fp = fingerprint("checkout", expected_version)
        keys = [cart_key(uid), idem_key(uid, idem), ORDERS_STREAM, EVENTS_STREAM,
                f"order:{order_id}", f"orders:{uid}"] + [f"product:{s}" for s in skus]
        args = [expected_version, order_id, uid, now_ms(), config.INSTANCE_ID, fp,
                config.IDEMPOTENCY_TTL_SECONDS, config.CART_TTL_SECONDS] + skus
        status, payload = await self.r.run_script("checkout", keys, args)
        if status in ("OK", "REPLAY"):
            self.cache.invalidate(uid)
            if status == "REPLAY":
                METRICS.inc("cart_idempotent_replays_total")
            else:
                METRICS.inc("orders_created_total")
            return json.loads(payload), status == "REPLAY"
        if status == "CONFLICT":
            METRICS.inc("cart_version_conflicts_total")
            raise StoreError(409, "VERSION_CONFLICT",
                             f"cart changed (expected version {expected_version}, current {payload})")
        raise _map_error(status, payload)


class NaiveCartStore:
    """BASELINE ONLY - demonstrates what goes wrong without the consistency design."""
    mode = "naive"

    def __init__(self, r: Redis, cache: L1Cache):
        self.r, self.cache = r, cache

    async def get(self, uid: str, min_version: int = 0) -> dict:
        hit = self.cache.get(uid)  # instance-local, never invalidated by other instances
        if hit is not None:
            return hit
        state = _parse_cart(await self.r.execute("HGETALL", cart_key(uid)))
        self.cache._d[uid] = _Entry(state)
        return state

    async def _save(self, uid: str, state: dict) -> None:
        # "save the whole cart object" - overwrites whatever another instance wrote
        cmds: list[tuple] = [("DEL", cart_key(uid))]
        fields = ["v", state["version"], "updated_at", now_ms(), "updated_by", config.INSTANCE_ID]
        for s, q in state["items"].items():
            fields += [f"item:{s}", q]
        cmds.append(("HSET", cart_key(uid), *fields))
        cmds.append(("EXPIRE", cart_key(uid), config.CART_TTL_SECONDS))
        await self.r.pipeline(cmds)
        self.cache._d[uid] = _Entry(state)

    async def mutate(self, uid: str, op: str, sku: str = "", qty: int = 0,
                     expected_version: int | None = None, idem: str = "") -> tuple[dict, bool]:
        state = await self.get(uid)
        state = {"version": state["version"], "items": dict(state["items"]),
                 "updated_at": state["updated_at"]}
        if op in ("add", "set") and not await self.r.execute("EXISTS", f"product:{sku}"):
            raise StoreError(404, "PRODUCT_NOT_FOUND", f"unknown sku {sku}")
        if op == "add":
            state["items"][sku] = state["items"].get(sku, 0) + qty
        elif op == "set":
            state["items"][sku] = qty
        elif op == "remove":
            state["items"].pop(sku, None)
        elif op == "clear":
            state["items"] = {}
        state["items"] = {k: v for k, v in state["items"].items() if v > 0}
        state["version"] += 1
        await self._save(uid, state)
        return state, False

    async def checkout(self, uid: str, expected_version: int, idem: str = "") -> tuple[dict, bool]:
        state = await self.get(uid)
        if not state["items"]:
            raise StoreError(409, "CART_EMPTY", "cart is empty")
        lines, total = [], 0
        for sku, q in state["items"].items():
            p = await self.r.execute("HMGET", f"product:{sku}", "stock", "price_cents", "name")
            if int(p[0]) < q:
                raise StoreError(409, "OUT_OF_STOCK", f"not enough stock for {sku}")
            lines.append({"sku": sku, "qty": q, "price_cents": int(p[1]), "name": p[2]})
            total += q * int(p[1])
        for line in lines:  # check-then-act: another instance can sell the same units
            stock = int(await self.r.execute("HGET", f"product:{line['sku']}", "stock"))
            await self.r.execute("HSET", f"product:{line['sku']}", "stock", stock - line["qty"])
        order_id = "o_" + uuid.uuid4().hex[:12]
        await self.r.execute("HSET", f"order:{order_id}", "id", order_id, "user", uid,
                             "status", "PENDING", "items", json.dumps(lines),
                             "total_cents", total, "created_at", now_ms(),
                             "created_by", config.INSTANCE_ID)
        await self.r.execute("ZADD", f"orders:{uid}", now_ms(), order_id)
        await self.r.execute("XADD", ORDERS_STREAM, "*", "order_id", order_id, "user", uid)
        state = {"version": state["version"] + 1, "items": {}, "updated_at": now_ms()}
        await self._save(uid, state)
        METRICS.inc("orders_created_total")
        return {"order_id": order_id, "status": "PENDING", "total_cents": total,
                "items": lines, "cart_version": state["version"]}, False


def _map_error(status: str, payload: str) -> StoreError:
    if status == "NO_PRODUCT":
        return StoreError(404, "PRODUCT_NOT_FOUND", f"unknown product {payload}".strip())
    if status == "NO_ITEM":
        return StoreError(404, "ITEM_NOT_IN_CART", "item is not in the cart")
    if status == "TOO_MANY":
        return StoreError(422, "QUANTITY_LIMIT", f"max {config.MAX_QTY_PER_ITEM} per item")
    if status == "EMPTY":
        return StoreError(409, "CART_EMPTY", "cart is empty")
    if status == "OUT_OF_STOCK":
        sku, have, want = payload.split(":")
        METRICS.inc("checkout_out_of_stock_total")
        return StoreError(409, "OUT_OF_STOCK", f"only {have} left of {sku}, cart has {want}")
    if status == "IDEM_MISMATCH":
        return StoreError(422, "IDEMPOTENCY_KEY_REUSED",
                          "Idempotency-Key was already used with a different request")
    return StoreError(500, "INTERNAL", f"unexpected script status {status}")


# --------------------------------------------------------------------------
class OrderStore:
    def __init__(self, r: Redis):
        self.r = r

    async def get(self, order_id: str) -> dict | None:
        flat = await self.r.execute("HGETALL", f"order:{order_id}")
        if not flat:
            return None
        d = dict(zip(flat[::2], flat[1::2]))
        d["items"] = json.loads(d.get("items", "[]"))
        d["total_cents"] = int(d.get("total_cents", 0))
        return d

    async def list_for(self, uid: str, limit: int = 50) -> list[dict]:
        ids = await self.r.execute("ZREVRANGE", f"orders:{uid}", 0, limit - 1) or []
        out = []
        for oid in ids:
            o = await self.get(oid)
            if o:
                out.append(o)
        return out
