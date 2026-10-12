# API documentation — Distributed Shopping Cart Consistency System

Base URL: `http://<load-balancer>/api/v1` (local: `http://localhost:8080/api/v1`).
All bodies are JSON. Machine-readable spec: [`openapi.yaml`](openapi.yaml).

## Conventions

| Header | Direction | Meaning |
|---|---|---|
| `Authorization: Bearer <JWT>` | request | Required on every endpoint except `/health`, `/metrics`, `/auth/*`, `GET /products`. HS256 JWT, 1 h lifetime, claims `sub`, `name`, `role`, `exp`, `iss=dscart`. |
| `Idempotency-Key: <8-100 chars>` | request | Makes a write safe to retry. The first result is stored for 24 h; a retry with the same key returns that stored result instead of applying the change again. Re-using a key with a *different* body → `422 IDEMPOTENCY_KEY_REUSED`. |
| `If-Match: "<version>"` | request | Optimistic concurrency. The write only succeeds if the cart is still at that version; otherwise `409 VERSION_CONFLICT` with the current cart. **Required** for `PUT /cart/items/{sku}` and `POST /cart/checkout` (`428` if missing); optional for adds/removes. |
| `X-Min-Version: <n>` | request (GET /cart) | Read-your-writes token: the instance will not serve a cached cart older than version *n*. |
| `ETag: "<version>"` | response | Current cart version. Send it back as `If-Match`. |
| `Idempotent-Replayed: true` | response | This response is a replay of an earlier identical request. |
| `X-Instance`, `X-Request-Id`, `X-Consistency-Mode` | response | Which instance served the request, a correlation id that also appears in the logs, and the active strategy. |

### Roles (RBAC)

| Role | Can do |
|---|---|
| `customer` | own cart and own orders only |
| `admin` | manage products, view cluster/events/raw carts, configure chaos (failure injection) |
| `service` | read-only operational views (`/admin/cluster`, `/admin/events`, `/admin/carts/{id}`) — for monitoring |

### Error format

```json
{"error": "VERSION_CONFLICT", "message": "cart changed (expected version 2, current 3); re-read and retry",
 "current": { "...current cart, only for conflicts..." }}
```

| Status | `error` codes |
|---|---|
| 400 | `BAD_JSON`, `BAD_IF_MATCH`, `BAD_IDEMPOTENCY_KEY` |
| 401 | `UNAUTHENTICATED`, `INVALID_TOKEN`, `TOKEN_EXPIRED`, `BAD_CREDENTIALS` |
| 403 | `FORBIDDEN` |
| 404 | `PRODUCT_NOT_FOUND`, `ITEM_NOT_IN_CART`, `ORDER_NOT_FOUND` |
| 409 | `VERSION_CONFLICT`, `OUT_OF_STOCK`, `CART_EMPTY`, `USERNAME_TAKEN` |
| 422 | `VALIDATION`, `QUANTITY_LIMIT`, `IDEMPOTENCY_KEY_REUSED` |
| 428 | `PRECONDITION_REQUIRED` |
| 503 | `STORAGE_UNAVAILABLE` (with `Retry-After: 1`) |

---

## Endpoints (18)

| # | Method & path | Auth | Purpose |
|---|---|---|---|
| 1 | `POST /auth/register` | – | create customer account, returns JWT |
| 2 | `POST /auth/login` | – | exchange credentials for JWT |
| 3 | `GET /products` | – | catalogue with price & stock |
| 4 | `POST /products` | admin | create/update a product |
| 5 | `GET /cart` | customer | current cart (ETag = version) |
| 6 | `POST /cart/items` | customer | add quantity of a SKU (merge-safe increment) |
| 7 | `PUT /cart/items/{sku}` | customer | set absolute quantity (If-Match required) |
| 8 | `DELETE /cart/items/{sku}` | customer | remove a line |
| 9 | `DELETE /cart` | customer | empty the cart |
| 10 | `POST /cart/checkout` | customer | atomic checkout → order (If-Match required) |
| 11 | `GET /orders` | customer | own orders, newest first |
| 12 | `GET /orders/{id}` | customer/admin | one order |
| 13 | `GET /admin/cluster` | admin/service | live instances, Redis role/replicas, cluster counters |
| 14 | `GET /admin/events?count=&user=` | admin/service | audit stream of cart changes |
| 15 | `GET /admin/carts/{user_id}` | admin/service | raw Redis hash + TTL of a cart |
| 16 | `POST /admin/chaos` | admin | failure injection on an instance |
| 17 | `GET /health` | – | liveness + Redis connectivity (LB/Docker health check) |
| 18 | `GET /metrics` | – | Prometheus metrics of the instance |

### 1. `POST /auth/register`
```http
POST /api/v1/auth/register
{"username": "alice", "password": "alice-pass-1"}
```
`201` → `{"id":"u_864066b0de3b","username":"alice","role":"customer","token":"eyJ..."}`
Errors: `409 USERNAME_TAKEN` (registration is race-free: `HSETNX` on the username index), `422 VALIDATION`.

### 2. `POST /auth/login`
`{"username":"alice","password":"alice-pass-1"}` → `200 {"id":..., "role":"customer", "token":"eyJ...", "expires_in":3600}` · `401 BAD_CREDENTIALS`.

### 3. `GET /products`
`200 {"products":[{"sku":"SKU-CAP","name":"Baseball Cap","price_cents":29900,"stock":100000}, ...]}`
The list is cached per instance for 60 s (stock shown is informational; the authoritative stock check happens atomically at checkout).

### 4. `POST /products` (admin)
`{"sku":"SKU-LIMITED","name":"Limited Edition Hoodie","price_cents":259900,"stock":5}` → `201` same object. Publishes `product-invalidate` so every instance drops its catalogue cache.

### 5. `GET /cart`
Optional header `X-Min-Version: 7`.
```json
200  ETag: "2"
{"version":2,
 "items":[{"sku":"SKU-SNEAKER","name":"Running Sneakers","qty":1,"unit_price_cents":349900,"line_total_cents":349900},
          {"sku":"SKU-TSHIRT","name":"Cotton T-Shirt","qty":2,"unit_price_cents":49900,"line_total_cents":99800}],
 "item_count":3,"total_cents":449700,"updated_at":"1791798273082"}
```

### 6. `POST /cart/items`
```http
POST /api/v1/cart/items
Idempotency-Key: 6f1c1d2e-add-01
{"sku":"SKU-TSHIRT","qty":2}
```
`200` + cart (`ETag` = new version). `qty` 1–1000. This is a **relative** change (`HINCRBY` inside the Lua script), so adds from many devices/instances merge without needing `If-Match`; send `If-Match` if the client wants strict ordering.
Retry with the same key → `200`, `Idempotent-Replayed: true`, same body, cart unchanged. Errors: `404 PRODUCT_NOT_FOUND`, `409 VERSION_CONFLICT` (only if If-Match given), `422`.

### 7. `PUT /cart/items/{sku}`
```http
PUT /api/v1/cart/items/SKU-TSHIRT
If-Match: "2"
{"qty": 5}
```
`200` + cart. `qty: 0` removes the line. Stale version → `409 VERSION_CONFLICT` with `current` cart and its `ETag`; no `If-Match` → `428`.

### 8. `DELETE /cart/items/{sku}`
`200` + cart · `404 ITEM_NOT_IN_CART`. Optional `If-Match`, `Idempotency-Key`.

### 9. `DELETE /cart`
`200` + empty cart (version still increases — versions never go backwards).

### 10. `POST /cart/checkout`
```http
POST /api/v1/cart/checkout
If-Match: "3"
Idempotency-Key: 9a7e-checkout-01
```
`201 Location: /api/v1/orders/o_7db6348d901f`
```json
{"order_id":"o_7db6348d901f","status":"PENDING","total_cents":599400,"cart_version":4,
 "items":[{"sku":"SKU-SNEAKER","qty":1,"name":"Running Sneakers","price_cents":349900}, ...]}
```
In one atomic step: version check → stock check for every line → stock decrement → order + index + queue message → cart emptied (version+1). Errors: `409 VERSION_CONFLICT`, `409 OUT_OF_STOCK` (nothing reserved), `409 CART_EMPTY`, `428`.
The order worker later sets `status: CONFIRMED`.

### 11–12. `GET /orders`, `GET /orders/{id}`
```json
{"id":"o_7db6348d901f","user":"u_864066b0de3b","status":"CONFIRMED","total_cents":599400,
 "items":[...],"cart_version":"3","created_by":"app-2","confirmed_by":"worker-1",
 "created_at":"1791798273284","confirmed_at":"1791798273310"}
```
Customers only see their own orders (`404` otherwise).

### 13. `GET /admin/cluster`
```json
{"live_instances":[{"id":"app-1","mode":"consistent","pid":2662,"started_at":1791798239,"last_seen":1791798277}, ...],
 "cluster_counters":{"version_conflicts":1,"orders_confirmed":6,"chaos_crashes":0},
 "redis":{"role":"master","connected_replicas":1,"events_in_stream":33}}
```
An instance disappears from `live_instances` ~6 s after it stops heart-beating.

### 14. `GET /admin/events?count=4&user=u_864066b0de3b`
`{"events":[{"id":"1791798275715-0","user":"u_...","op":"add","sku":"SKU-CAP","qty":"1","version":"8","instance":"app-3","ts":"1791798275715"}, ...]}`

### 15. `GET /admin/carts/{user_id}`
`{"key":"cart:{u_...}","ttl_seconds":604763,"hash":{"v":"8","item:SKU-CAP":"4","updated_at":"...","updated_by":"app-3"}}`

### 16. `POST /admin/chaos`
```json
{"instance":"app-2","mode":"crash_after_commit","probability":0.25,"ttl_seconds":8}
```
Modes: `off`, `crash_after_commit` (commit in Redis, then the process exits before replying), `latency` (`latency_ms` added to every request), `crash_now`. `instance` defaults to `all`. Picked up by instances within one heartbeat (2 s).

### 17. `GET /health`
`200 {"status":"ok","instance":"app-1","mode":"consistent","redis":true,"uptime_s":12.3}` · `503` when Redis is unreachable.

### 18. `GET /metrics`
Prometheus text format. Main series:

| Metric | Meaning |
|---|---|
| `cart_http_requests_total{method,route,status}` | request count |
| `cart_http_request_duration_seconds_bucket{route,le}` | latency histogram |
| `cart_version_conflicts_total` | writes rejected by optimistic concurrency |
| `cart_idempotent_replays_total` | retries answered from the idempotency store |
| `cart_cache_hits_total` / `cart_cache_misses_total` | L1 cache effectiveness |
| `cart_cache_invalidations_total` | entries evicted by pub/sub messages |
| `orders_created_total`, `checkout_out_of_stock_total`, `storage_errors_total` | business/failure counters |

---

## Typical client flow

```bash
TOKEN=$(curl -s -XPOST $API/auth/login -d '{"username":"alice","password":"alice-pass-1"}' | jq -r .token)
curl -XPOST $API/cart/items -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $(uuidgen)" \
     -d '{"sku":"SKU-TSHIRT","qty":2}'                     # -> ETag: "1"
curl -XPUT  $API/cart/items/SKU-TSHIRT -H "Authorization: Bearer $TOKEN" -H 'If-Match: "1"' -d '{"qty":3}'
curl -XPOST $API/cart/checkout -H "Authorization: Bearer $TOKEN" -H 'If-Match: "2"' -H "Idempotency-Key: $(uuidgen)"
```
On `409 VERSION_CONFLICT`: take `current` from the body (or `GET /cart`), re-apply the user's intent, retry with the new `ETag`.
On a network error/timeout: retry **with the same Idempotency-Key**.
