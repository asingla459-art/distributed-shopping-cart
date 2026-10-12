"""Closed-loop load generator: N virtual users, each with its own account and cart.

Request mix per iteration (realistic shopping behaviour):
  50 %  GET  /cart                    (with X-Min-Version read-your-writes token)
  30 %  POST /cart/items              (+qty, Idempotency-Key)
  10 %  PUT  /cart/items/{sku}        (absolute qty, If-Match from last ETag)
   5 %  DELETE /cart/items/{sku}
   5 %  POST /cart/checkout           (If-Match + Idempotency-Key)

usage: python3 -m loadtest.loadgen --users 25 --duration 20 --label medium [--out file.json]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import time
import uuid
from pathlib import Path

from .httpclient import Client, new_user, pct

SKUS = ["SKU-TSHIRT", "SKU-JEANS", "SKU-SNEAKER", "SKU-CAP", "SKU-WATCH", "SKU-BOTTLE", "SKU-HEADPHONE"]
INSTANCE_PORTS = [8001, 8002, 8003]


def scrape(text: str, name: str) -> float:
    return sum(float(m) for m in re.findall(rf"^{name}{{[^}}]*}} ([0-9.e+]+)$", text, re.M))


async def instance_counters() -> dict:
    tot = {"hits": 0.0, "misses": 0.0}
    for p in INSTANCE_PORTS:
        try:
            c = Client(port=p, timeout=3)
            t = (await c.request("GET", "/metrics")).body.decode()
            await c.close()
            tot["hits"] += scrape(t, "cart_cache_hits_total")
            tot["misses"] += scrape(t, "cart_cache_misses_total")
        except Exception:
            pass
    return tot


async def vu(c: Client, stop_at: float, records: list, rng: random.Random) -> None:
    version = 0
    while time.time() < stop_at:
        x = rng.random()
        sku = rng.choice(SKUS)
        try:
            if x < 0.50:
                op = "get"
                r = await c.request("GET", "/api/v1/cart", None, {"X-Min-Version": str(version)})
            elif x < 0.80:
                op = "add"
                r = await c.request("POST", "/api/v1/cart/items", {"sku": sku, "qty": rng.randint(1, 3)},
                                    {"Idempotency-Key": "lg-" + uuid.uuid4().hex})
            elif x < 0.90:
                op = "set"
                r = await c.request("PUT", f"/api/v1/cart/items/{sku}", {"qty": rng.randint(1, 5)},
                                    {"If-Match": f'"{version}"'})
            elif x < 0.95:
                op = "remove"
                r = await c.request("DELETE", f"/api/v1/cart/items/{sku}")
            else:
                op = "checkout"
                r = await c.request("POST", "/api/v1/cart/checkout", None,
                                    {"If-Match": f'"{version}"', "Idempotency-Key": "co-" + uuid.uuid4().hex})
            et = r.headers.get("etag")
            if et:
                version = max(version, int(et.strip('"')))
            elif op == "checkout" and r.status == 201:
                version = r.json()["cart_version"]
            code = r.json().get("error", "") if r.status >= 400 else ""
            records.append((time.time(), op, r.status, r.ms, r.headers.get("x-instance", "?"),
                            int(r.headers.get("x-upstream-tries", "1")),
                            r.headers.get("idempotent-replayed") == "true", code))
        except Exception as e:  # connection failure visible to the client
            records.append((time.time(), "error", 0, 0.0, type(e).__name__, 0, False, type(e).__name__))
            await asyncio.sleep(0.05)


def summarize(label: str, users: int, records: list, duration: float, cache: dict) -> dict:
    ok = [r for r in records if 200 <= r[2] < 300]
    lat = [r[3] for r in records if r[2] > 0]
    by_op = {}
    for op in ("get", "add", "set", "remove", "checkout"):
        l = [r[3] for r in records if r[1] == op and r[2] > 0]
        if l:
            by_op[op] = {"count": len(l), "p50_ms": round(pct(l, 50), 2), "p95_ms": round(pct(l, 95), 2)}
    statuses = {}
    for r in records:
        statuses[str(r[2])] = statuses.get(str(r[2]), 0) + 1
    hits, misses = cache.get("hits", 0), cache.get("misses", 0)
    return {
        "label": label, "users": users, "duration_s": round(duration, 1),
        "requests": len(records), "throughput_rps": round(len(records) / duration, 1),
        "success_rps": round(len(ok) / duration, 1),
        "p50_ms": round(pct(lat, 50), 2), "p95_ms": round(pct(lat, 95), 2), "p99_ms": round(pct(lat, 99), 2),
        "mean_ms": round(sum(lat) / max(len(lat), 1), 2),
        "server_errors_pct": round(100 * sum(1 for r in records if r[2] >= 500 or r[2] == 0) / max(len(records), 1), 3),
        "version_conflict_pct": round(100 * sum(1 for r in records if r[7] == "VERSION_CONFLICT")
                                      / max(len(records), 1), 3),
        "error_codes": {c: sum(1 for r in records if r[7] == c) for c in sorted({r[7] for r in records if r[7]})},
        "statuses": statuses, "by_op": by_op,
        "lb_retried_requests": sum(1 for r in records if r[5] > 1),
        "idempotent_replays": sum(1 for r in records if r[6]),
        "cache_hit_ratio_pct": round(100 * hits / (hits + misses), 1) if hits + misses else None,
        "per_instance": {i: sum(1 for r in records if r[4] == i) for i in sorted({r[4] for r in records})},
    }


async def run(users: int, duration: float, label: str, seed: int = 7) -> tuple[dict, list]:
    clients = await asyncio.gather(*(new_user(f"lg{label}{int(time.time())}_", i) for i in range(users)))
    before = await instance_counters()
    records: list = []
    t0 = time.time()
    stop_at = t0 + duration
    await asyncio.gather(*(vu(c, stop_at, records, random.Random(seed + i)) for i, c in enumerate(clients)))
    dur = time.time() - t0
    after = await instance_counters()
    cache = {k: after[k] - before[k] for k in before}
    for c in clients:
        await c.close()
    return summarize(label, users, records, dur, cache), records


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=10)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--label", default="run")
    ap.add_argument("--out")
    a = ap.parse_args()
    summary, _ = asyncio.run(run(a.users, a.duration, a.label))
    print(json.dumps(summary, indent=2))
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
