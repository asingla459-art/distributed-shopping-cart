"""Controlled failure experiment (run against the local cluster).

Timeline (35 s of continuous load, 20 virtual users, '+1' adds with Idempotency-Keys):
  t=8s   app-2 gets chaos mode 'crash_after_commit' (p=0.25) for 8 s: it commits
         the write in Redis, then dies before replying. The LB retries the request
         on another instance; the supervisor restarts app-2 (2 s).
  t=20s  app-3 is hard-killed (SIGKILL) and auto-restarted 2 s later.
Clients retry failed requests up to 5 times with the SAME Idempotency-Key.

Correctness check at the end: for every user, quantity stored in Redis must equal
the number of adds the user saw acknowledged (no lost and no duplicated adds).

Part B: order-worker crash. A worker dies while holding orders; a second worker
reclaims them from the consumer group's pending list (XAUTOCLAIM).

usage: python3 -m experiments.failure <label>
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

from loadtest.httpclient import Client, login, new_user

ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(__file__).parent / "results"
DURATION = 35
USERS = 20


def rcli(*args) -> str:
    return subprocess.check_output(["redis-cli", *map(str, args)]).decode().strip()


async def vu(c: Client, t0: float, rec: list, acked: dict) -> None:
    while time.time() - t0 < DURATION:
        key = "f-" + uuid.uuid4().hex
        for attempt in range(6):
            try:
                r = await c.request("POST", "/api/v1/cart/items", {"sku": "SKU-CAP", "qty": 1},
                                    {"Idempotency-Key": key})
                st, inst, tries = r.status, r.headers.get("x-instance", "?"), int(r.headers.get("x-upstream-tries", "1"))
                replay = r.headers.get("idempotent-replayed") == "true"
            except Exception as e:
                st, inst, tries, replay = 0, type(e).__name__, 0, False
            rec.append((time.time() - t0, st, inst, tries, replay, attempt))
            if st == 200:
                acked[c.user_id] += 1
                break
            await asyncio.sleep(0.2 * (attempt + 1))


async def part_a(label: str) -> dict:
    adm = await login("admin", "admin-pass-123")
    users = [await new_user(f"fx{label}{int(time.time())}_", i) for i in range(USERS)]
    acked = {u.user_id: 0 for u in users}
    rec: list = []
    marks = []
    t0 = time.time()

    async def chaos_script():
        await asyncio.sleep(8)
        r = await adm.request("POST", "/api/v1/admin/chaos",
                              {"instance": "app-2", "mode": "crash_after_commit",
                               "probability": 0.25, "ttl_seconds": 8})
        marks.append({"t": round(time.time() - t0, 2), "event": "chaos crash_after_commit on app-2 (8 s)",
                      "status": r.status})
        await asyncio.sleep(12)
        subprocess.run([str(ROOT / "deploy/local/cluster.sh"), "kill", "3"], check=True,
                       capture_output=True)
        marks.append({"t": round(time.time() - t0, 2), "event": "SIGKILL app-3"})

    await asyncio.gather(chaos_script(), *(vu(u, t0, rec, acked) for u in users))
    await asyncio.sleep(1)
    stored = {u: int(rcli("HGET", f"cart:{{{u}}}", "item:SKU-CAP") or 0) for u in acked}
    mismatched = {u: stored[u] - acked[u] for u in acked if stored[u] != acked[u]}

    # timeline per second
    secs = int(DURATION) + 1
    tl = [{"t": s, "ok": 0, "failed_attempts": 0, "lb_retried": 0, "replays": 0,
           "by_instance": {}} for s in range(secs)]
    for t, st, inst, tries, replay, attempt in rec:
        b = tl[min(int(t), secs - 1)]
        if st == 200:
            b["ok"] += 1
            b["by_instance"][inst] = b["by_instance"].get(inst, 0) + 1
        else:
            b["failed_attempts"] += 1
        b["lb_retried"] += tries > 1
        b["replays"] += replay
    kill_t = next((m["t"] for m in marks if "SIGKILL" in m["event"]), None)
    back = next((t for t, st, inst, *_ in rec if kill_t and t > kill_t + 0.5 and inst == "app-3" and st == 200), None)
    sup = (ROOT / "run/supervisor.log").read_text() if (ROOT / "run/supervisor.log").exists() else ""
    return {
        "experiment": "Instance failures under load", "mode": label, "users": USERS,
        "duration_s": DURATION, "marks": marks,
        "logical_adds_acknowledged": sum(acked.values()),
        "stored_total": sum(stored.values()),
        "users_with_wrong_cart": len(mismatched), "net_wrong_units": sum(mismatched.values()),
        "client_visible_failed_attempts": sum(1 for r in rec if r[1] != 200),
        "requests_retried_by_lb": sum(1 for r in rec if r[3] > 1),
        "idempotent_replays": sum(1 for r in rec if r[4]),
        "instance_crashes": sup.count("exited"),
        "app3_back_in_rotation_after_s": round(back - kill_t, 2) if back and kill_t else None,
        "timeline": tl,
    }


async def part_b() -> dict:
    """Order worker dies holding orders -> another worker reclaims them."""
    pidf = ROOT / "run/worker-1.pid"
    if pidf.exists():
        subprocess.run(["kill", pidf.read_text().strip()])
    await asyncio.sleep(0.5)
    ids = []
    for i in range(5):
        u = await new_user(f"wb{int(time.time())}_", i)
        r = await u.request("POST", "/api/v1/cart/items", {"sku": "SKU-WATCH", "qty": 1},
                            {"Idempotency-Key": "wb-" + uuid.uuid4().hex})
        r = await u.request("POST", "/api/v1/cart/checkout", None,
                            {"If-Match": f'"{r.json()["version"]}"', "Idempotency-Key": "wb-" + uuid.uuid4().hex})
        ids.append(r.json()["order_id"])
    # the crashy worker reads all 5 orders in one batch and dies on the first one
    env = {**os.environ, "WORKER_ID": "crashy-worker", "WORKER_FAIL_RATE": "1.0"}
    crashy = subprocess.Popen([sys.executable, "-m", "app.worker"], cwd=ROOT, env=env,
                              stdout=open(ROOT / "run/worker-crashy.out", "w"), stderr=subprocess.STDOUT)
    crashy.wait(timeout=10)
    await asyncio.sleep(0.5)
    pending = int(rcli("XPENDING", "stream:orders", "order-workers").split("\n")[0])
    statuses_before = [rcli("HGET", f"order:{o}", "status") for o in ids]
    t_start = time.time()
    env = {**os.environ, "WORKER_ID": "worker-2", "CLAIM_IDLE_MS": "3000"}
    healthy = subprocess.Popen([sys.executable, "-m", "app.worker"], cwd=ROOT, env=env,
                               stdout=open(ROOT / "run/worker-2.out", "w"), stderr=subprocess.STDOUT)
    (ROOT / "run/worker-2.pid").write_text(str(healthy.pid))
    while time.time() - t_start < 20:
        st = [rcli("HGET", f"order:{o}", "status") for o in ids]
        if all(s == "CONFIRMED" for s in st):
            break
        await asyncio.sleep(0.25)
    return {"experiment": "Order worker crash", "orders": len(ids),
            "crashy_worker_exit_code": crashy.returncode,
            "pending_after_crash": pending, "statuses_after_crash": statuses_before,
            "statuses_after_recovery": st,
            "confirmed_by": [rcli("HGET", f"order:{o}", "confirmed_by") for o in ids],
            "recovery_seconds": round(time.time() - t_start, 2),
            "pending_after_recovery": int(rcli("XPENDING", "stream:orders", "order-workers").split("\n")[0])}


async def main(label: str) -> None:
    a = await part_a(label)
    out = {"part_a": a}
    if label == "consistent":
        out["part_b"] = await part_b()
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"failure_{label}.json").write_text(json.dumps(out, indent=2))
    print(json.dumps({k: v for k, v in a.items() if k != "timeline"}, indent=2))
    if "part_b" in out:
        print(json.dumps(out["part_b"], indent=2))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
