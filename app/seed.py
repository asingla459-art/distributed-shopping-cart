"""Seed the catalogue and an admin account.  python -m app.seed"""
import asyncio
import os

from . import config
from .redis_client import Redis
from .store import ProductStore, StoreError, UserStore

PRODUCTS = [
    ("SKU-TSHIRT", "Cotton T-Shirt", 49900, 100000),
    ("SKU-JEANS", "Slim Fit Jeans", 189900, 100000),
    ("SKU-SNEAKER", "Running Sneakers", 349900, 100000),
    ("SKU-CAP", "Baseball Cap", 29900, 100000),
    ("SKU-WATCH", "Analog Watch", 499900, 100000),
    ("SKU-BOTTLE", "Steel Water Bottle", 59900, 100000),
    ("SKU-HEADPHONE", "Wireless Headphones", 299900, 100000),
    ("SKU-LIMITED", "Limited Edition Hoodie", 259900, 5),   # used in the oversell demo
]


async def main() -> None:
    r = Redis(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD, pool_size=4)
    ps, us = ProductStore(r), UserStore(r)
    for sku, name, price, stock in PRODUCTS:
        await ps.upsert(sku, name, price, stock)
    for name, pw, role in [
        ("admin", os.environ.get("ADMIN_PASSWORD", "admin-pass-123"), "admin"),
        ("monitor", os.environ.get("MONITOR_PASSWORD", "monitor-pass-123"), "service"),
    ]:
        try:
            await us.create(name, pw, role)
            print(f"created {role} user '{name}'")
        except StoreError:
            print(f"user '{name}' already exists")
    print(f"seeded {len(PRODUCTS)} products")
    await r.close()


if __name__ == "__main__":
    asyncio.run(main())
