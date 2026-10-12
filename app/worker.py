"""Order worker: consumes `stream:orders` through a Redis consumer group.

* Each order is processed by exactly one worker of the group.
* A worker acknowledges (XACK) only after the order is CONFIRMED.
* If a worker dies mid-order, the entry stays in the Pending Entries List and
  another worker reclaims it with XAUTOCLAIM after CLAIM_IDLE_MS.
* Processing is idempotent (status check before update), so a reclaimed
  order is never confirmed twice.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import time

from . import config
from .logging_setup import event, setup
from .redis_client import Redis, RedisError
from .store import ORDERS_STREAM

GROUP = "order-workers"
CLAIM_IDLE_MS = int(os.environ.get("CLAIM_IDLE_MS", "10000"))
CONSUMER = os.environ.get("WORKER_ID", f"worker-{os.getpid()}")
FAIL_RATE = float(os.environ.get("WORKER_FAIL_RATE", "0"))  # chaos: die mid-order

log = setup()


async def process(r: Redis, entry_id: str, fields: dict) -> None:
    oid = fields["order_id"]
    status = await r.execute("HGET", f"order:{oid}", "status")
    if status == "CONFIRMED":  # already done by a previous attempt
        await r.execute("XACK", ORDERS_STREAM, GROUP, entry_id)
        event(log, "order_already_confirmed", order=oid)
        return
    await asyncio.sleep(0.02)  # simulated payment / fulfilment call
    if FAIL_RATE and random.random() < FAIL_RATE:
        event(log, "worker_chaos_crash", logging.WARNING, order=oid)
        os._exit(17)
    await r.pipeline([
        ("HSET", f"order:{oid}", "status", "CONFIRMED", "confirmed_at", int(time.time() * 1000),
         "confirmed_by", CONSUMER),
        ("XACK", ORDERS_STREAM, GROUP, entry_id),
        ("HINCRBY", "metrics:cluster", "orders_confirmed", 1),
    ])
    event(log, "order_confirmed", order=oid, user=fields.get("user"), entry=entry_id)


async def main() -> None:
    r = Redis(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD, pool_size=4)
    while True:
        try:
            await r.execute("XGROUP", "CREATE", ORDERS_STREAM, GROUP, "0", "MKSTREAM")
            break
        except RedisError as e:
            if "BUSYGROUP" in str(e):
                break
            raise
        except OSError:
            await asyncio.sleep(1)
    event(log, "worker_started", consumer=CONSUMER)
    while True:
        try:
            # 1. reclaim orders stuck with a dead worker
            res = await r.execute("XAUTOCLAIM", ORDERS_STREAM, GROUP, CONSUMER, CLAIM_IDLE_MS, "0-0",
                                  "COUNT", 50)
            for eid, flat in (res[1] if res else []) or []:
                if flat is None:
                    continue
                event(log, "order_reclaimed", logging.WARNING, entry=eid)
                await process(r, eid, dict(zip(flat[::2], flat[1::2])))
            # 2. new orders
            res = await r.execute("XREADGROUP", "GROUP", GROUP, CONSUMER, "COUNT", 50,
                                  "BLOCK", 2000, "STREAMS", ORDERS_STREAM, ">")
            for _stream, entries in res or []:
                for eid, flat in entries:
                    await process(r, eid, dict(zip(flat[::2], flat[1::2])))
        except (OSError, ConnectionError, RedisError) as e:
            event(log, "worker_storage_error", logging.ERROR, error=str(e))
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
