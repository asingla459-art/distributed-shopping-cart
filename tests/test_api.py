"""Integration tests against a running cluster (consistent mode).

    deploy/local/cluster.sh start consistent 3
    python3 -m unittest -v tests.test_api
"""
import asyncio
import subprocess
import unittest
import uuid
import warnings

from loadtest.httpclient import Client, login, new_user

API = "/api/v1"


def run(coro):
    return asyncio.run(coro)


def key() -> dict:
    return {"Idempotency-Key": "t-" + uuid.uuid4().hex}


class CartApiTest(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)

    def test_register_login_and_auth_required(self):
        async def go():
            u = await new_user("t_auth", uuid.uuid4().int % 10**8)
            anon = Client()
            r = await anon.request("GET", f"{API}/cart")
            self.assertEqual(r.status, 401)
            r = await u.request("GET", f"{API}/cart")
            self.assertEqual(r.status, 200)
            self.assertEqual(r.headers["etag"], '"0"')
            bad = Client()
            bad.token = u.token[:-2] + "xx"
            self.assertEqual((await bad.request("GET", f"{API}/cart")).status, 401)
        run(go())

    def test_rbac(self):
        async def go():
            u = await new_user("t_rbac", uuid.uuid4().int % 10**8)
            self.assertEqual((await u.request("GET", f"{API}/admin/cluster")).status, 403)
            self.assertEqual((await u.request("POST", f"{API}/products",
                                              {"sku": "X", "name": "x", "price_cents": 1, "stock": 1})).status, 403)
            adm = await login("admin", "admin-pass-123")
            self.assertEqual((await adm.request("GET", f"{API}/admin/cluster")).status, 200)
            mon = await login("monitor", "monitor-pass-123")
            self.assertEqual((await mon.request("GET", f"{API}/admin/cluster")).status, 200)
            self.assertEqual((await mon.request("POST", f"{API}/admin/chaos", {"mode": "off"})).status, 403)
        run(go())

    def test_idempotent_retry_is_not_applied_twice(self):
        async def go():
            u = await new_user("t_idem", uuid.uuid4().int % 10**8)
            h = key()
            r1 = await u.request("POST", f"{API}/cart/items", {"sku": "SKU-CAP", "qty": 3}, h)
            r2 = await u.request("POST", f"{API}/cart/items", {"sku": "SKU-CAP", "qty": 3}, h)
            self.assertEqual(r2.headers.get("idempotent-replayed"), "true")
            self.assertEqual(r1.json(), r2.json())
            cart = (await u.request("GET", f"{API}/cart")).json()
            self.assertEqual(cart["items"][0]["qty"], 3)
            r3 = await u.request("POST", f"{API}/cart/items", {"sku": "SKU-CAP", "qty": 9}, h)
            self.assertEqual(r3.status, 422)
        run(go())

    def test_stale_if_match_conflicts(self):
        async def go():
            u = await new_user("t_conf", uuid.uuid4().int % 10**8)
            r = await u.request("POST", f"{API}/cart/items", {"sku": "SKU-JEANS", "qty": 1}, key())
            v = r.headers["etag"]
            ok = await u.request("PUT", f"{API}/cart/items/SKU-JEANS", {"qty": 4}, {"If-Match": v})
            self.assertEqual(ok.status, 200)
            stale = await u.request("PUT", f"{API}/cart/items/SKU-JEANS", {"qty": 7}, {"If-Match": v})
            self.assertEqual(stale.status, 409)
            self.assertEqual(stale.json()["current"]["items"][0]["qty"], 4)
            self.assertEqual((await u.request("PUT", f"{API}/cart/items/SKU-JEANS", {"qty": 7})).status, 428)
        run(go())

    def test_concurrent_adds_are_not_lost(self):
        async def go():
            u = await new_user("t_conc", uuid.uuid4().int % 10**8)
            devs = []
            for _ in range(10):
                c = Client()
                c.token = u.token
                devs.append(c)

            async def add(c):
                for _ in range(10):
                    await c.request("POST", f"{API}/cart/items", {"sku": "SKU-TSHIRT", "qty": 1}, key())
            await asyncio.gather(*(add(c) for c in devs))
            cart = (await u.request("GET", f"{API}/cart")).json()
            self.assertEqual(cart["items"][0]["qty"], 100)
            self.assertEqual(cart["version"], 100)
        run(go())

    def test_checkout_and_no_oversell(self):
        async def go():
            adm = await login("admin", "admin-pass-123")
            sku = "SKU-T" + uuid.uuid4().hex[:6]
            await adm.request("POST", f"{API}/products", {"sku": sku, "name": "Test", "price_cents": 100, "stock": 3})
            users = [await new_user(f"t_os{uuid.uuid4().hex[:5]}_", i) for i in range(8)]
            ver = {}
            for u in users:
                r = await u.request("POST", f"{API}/cart/items", {"sku": sku, "qty": 1}, key())
                ver[u.user_id] = r.headers["etag"]
            res = await asyncio.gather(*(u.request("POST", f"{API}/cart/checkout", None,
                                                   {"If-Match": ver[u.user_id], **key()}) for u in users))
            self.assertEqual(sum(r.status == 201 for r in res), 3)
            self.assertEqual(sum(r.status == 409 for r in res), 5)
            stock = int(subprocess.check_output(["redis-cli", "HGET", f"product:{sku}", "stock"]))
            self.assertEqual(stock, 0)
            winner = next(u for u, r in zip(users, res) if r.status == 201)
            cart = (await winner.request("GET", f"{API}/cart")).json()
            self.assertEqual(cart["items"], [])
        run(go())

    def test_validation(self):
        async def go():
            u = await new_user("t_val", uuid.uuid4().int % 10**8)
            self.assertEqual((await u.request("POST", f"{API}/cart/items", {"sku": "SKU-CAP", "qty": 0})).status, 422)
            self.assertEqual((await u.request("POST", f"{API}/cart/items", {"sku": "NOPE", "qty": 1})).status, 404)
            self.assertEqual((await u.request("POST", f"{API}/cart/checkout", None, {"If-Match": '"0"'})).status, 409)
        run(go())


if __name__ == "__main__":
    unittest.main()
