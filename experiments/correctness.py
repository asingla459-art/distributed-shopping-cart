"""Correctness experiments against a running cluster (LB on :8080, instances on :8001-8003).

E1 lost updates      - 30 concurrent clients of ONE cart, 20 '+1' adds each
E2 overselling       - 20 customers race to check out the last 5 units of a product
E3 stale reads       - write via one instance, read immediately via another
E4 duplicate retries - every add is retried once with the same Idempotency-Key
E5 blind overwrites  - two devices change the same line's quantity at once

usage: python3 -m experiments.correctness <label>   (cluster must already be running)
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

from loadtest.httpclient import Client, login, new_user

RESULTS = Path(__file__).parent / "results"
INSTANCE_PORTS = [8001, 8002, 8003]


async def admin() -> Client:
    return await login("admin", "admin-pass-123")


async def redis_qty(adm: Client, user_id: str, sku: str) -> int:
    r = await adm.request("GET", f"/api/v1/admin/carts/{user_id}")
    return int(r.json()["hash"].get(f"item:{sku}", 0))


def idem() -> dict:
    return {"Idempotency-Key": "k-" + uuid.uuid4().hex}


# ------------------------------------------------------------------ E1
async def e1_lost_updates(tag: str, clients=30, adds=20) -> dict:
    owner = await new_user(f"e1{tag}", int(time.time()))
    devices = []
    for _ in range(clients):
        c = Client()
        c.token = owner.token
        devices.append(c)
    ok = 0
    latencies = []

    async def run(c: Client):
        nonlocal ok
        for _ in range(adds):
            r = await c.request("POST", "/api/v1/cart/items", {"sku": "SKU-TSHIRT", "qty": 1}, idem())
            latencies.append(r.ms)
            ok += r.status == 200

    t0 = time.perf_counter()
    await asyncio.gather(*(run(c) for c in devices))
    dur = time.perf_counter() - t0
    actual = await redis_qty(await admin(), owner.user_id, "SKU-TSHIRT")
    expected = clients * adds
    return {"experiment": "E1 lost updates", "acknowledged_adds": ok, "expected_qty": expected,
            "stored_qty": actual, "lost_updates": ok - actual,
            "lost_pct": round(100 * (ok - actual) / max(ok, 1), 2), "duration_s": round(dur, 2)}


# ------------------------------------------------------------------ E2
async def e2_oversell(tag: str, buyers=20, stock=5) -> dict:
    adm = await admin()
    await adm.request("POST", "/api/v1/products",
                      {"sku": "SKU-LIMITED", "name": "Limited Edition Hoodie", "price_cents": 259900,
                       "stock": stock})
    users = [await new_user(f"e2{tag}{int(time.time())}_", i) for i in range(buyers)]
    versions = {}
    for u in users:
        r = await u.request("POST", "/api/v1/cart/items", {"sku": "SKU-LIMITED", "qty": 1}, idem())
        versions[u.user_id] = r.json()["version"]

    async def buy(u: Client):
        return await u.request("POST", "/api/v1/cart/checkout", None,
                               {"If-Match": f'"{versions[u.user_id]}"', **idem()})

    res = await asyncio.gather(*(buy(u) for u in users))
    created = sum(r.status == 201 for r in res)
    rejected = sum(r.status == 409 for r in res)
    left = int(subprocess.check_output(["redis-cli", "HGET", "product:SKU-LIMITED", "stock"]))
    return {"experiment": "E2 overselling", "initial_stock": stock, "buyers": buyers,
            "orders_created": created, "rejected_out_of_stock": rejected,
            "oversold_units": max(0, created - stock), "final_stock_in_redis": left}


# ------------------------------------------------------------------ E3
async def e3_stale_reads(tag: str, rounds=300, use_token=False) -> dict:
    u = await new_user(f"e3{tag}{'t' if use_token else ''}", int(time.time()))
    readers = {p: Client(port=p) for p in INSTANCE_PORTS}
    for c in readers.values():
        c.token = u.token
    # warm every instance's cache
    for c in readers.values():
        await c.request("GET", "/api/v1/cart")
    stale, max_lag = 0, 0
    for i in range(rounds):
        w = await u.request("POST", "/api/v1/cart/items", {"sku": "SKU-CAP", "qty": 1}, idem())
        wv = w.json()["version"]
        writer_inst = int(w.headers["x-instance"].split("-")[1])
        port = INSTANCE_PORTS[(writer_inst) % 3]  # a different instance than the writer
        h = {"X-Min-Version": str(wv)} if use_token else {}
        r = await readers[port].request("GET", "/api/v1/cart", None, h)
        rv = r.json()["version"]
        if rv < wv:
            stale += 1
            max_lag = max(max_lag, wv - rv)
    return {"experiment": "E3 stale reads" + (" (with X-Min-Version token)" if use_token else ""),
            "reads": rounds, "stale_reads": stale, "stale_pct": round(100 * stale / rounds, 2),
            "max_versions_behind": max_lag}


# ------------------------------------------------------------------ E4
async def e4_duplicates(tag: str, adds=200) -> dict:
    u = await new_user(f"e4{tag}", int(time.time()))
    retry_client = Client()
    retry_client.token = u.token
    replays = 0
    for _ in range(adds):
        h = idem()
        await u.request("POST", "/api/v1/cart/items", {"sku": "SKU-BOTTLE", "qty": 1}, h)
        # the client timed out and retries the SAME request (same key) - lands on another instance
        r = await retry_client.request("POST", "/api/v1/cart/items", {"sku": "SKU-BOTTLE", "qty": 1}, h)
        replays += r.headers.get("idempotent-replayed") == "true"
    actual = await redis_qty(await admin(), u.user_id, "SKU-BOTTLE")
    return {"experiment": "E4 duplicate retries", "logical_adds": adds, "requests_sent": adds * 2,
            "stored_qty": actual, "duplicates_applied": actual - adds, "replays_detected": replays}


# ------------------------------------------------------------------ E5
async def e5_blind_overwrite(tag: str, rounds=50) -> dict:
    u = await new_user(f"e5{tag}", int(time.time()))
    a, b = Client(port=8001), Client(port=8002)
    a.token = b.token = u.token
    await u.request("POST", "/api/v1/cart/items", {"sku": "SKU-JEANS", "qty": 1}, idem())
    silent, conflicts = 0, 0
    for i in range(rounds):
        va = (await a.request("GET", "/api/v1/cart")).headers.get("etag", '"0"')
        vb = (await b.request("GET", "/api/v1/cart")).headers.get("etag", '"0"')
        ra, rb = await asyncio.gather(
            a.request("PUT", "/api/v1/cart/items/SKU-JEANS", {"qty": 2 + i % 5}, {"If-Match": va}),
            b.request("PUT", "/api/v1/cart/items/SKU-JEANS", {"qty": 7 + i % 5}, {"If-Match": vb}))
        if ra.status == 200 and rb.status == 200:
            silent += 1          # one user's change was overwritten without anyone noticing
        conflicts += (ra.status == 409) + (rb.status == 409)
    return {"experiment": "E5 concurrent quantity edits", "rounds": rounds,
            "silent_overwrites": silent, "conflicts_reported_409": conflicts}


async def main(label: str) -> None:
    out = [await e1_lost_updates(label), await e2_oversell(label), await e3_stale_reads(label),
           await e3_stale_reads(label, use_token=True), await e4_duplicates(label),
           await e5_blind_overwrite(label)]
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"correctness_{label}.json").write_text(json.dumps(out, indent=2))
    for r in out:
        print(json.dumps(r))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
