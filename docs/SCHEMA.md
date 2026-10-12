# Data model & schema

Redis is used as a **NoSQL key-value / data-structure store**. Each entity is
stored in the Redis type that matches how it is accessed. The `{…}` parts of
key names are Redis Cluster *hash tags*. All keys of one user's cart share a
tag, so the system can later be sharded across a Redis Cluster without changing
the Lua scripts.

## Entity overview

| Entity | Key | Redis type | TTL | Written by | Read by |
|---|---|---|---|---|---|
| Cart | `cart:{<user_id>}` | HASH | 7 days, sliding (renewed on each write) | `cart_mutate.lua`, `checkout.lua` | `GET /cart`, scripts |
| Idempotency record | `idem:{<user_id>}:<key>` | STRING | 24 h | scripts | scripts (replay) |
| Product | `product:<sku>` | HASH | – | admin API, `checkout.lua` (stock) | catalogue, scripts |
| Product index | `products` | SET | – | admin API | catalogue |
| User | `user:<user_id>` | HASH | – | register | login |
| Username index | `users:by_name` | HASH (username → id) | – | register (`HSETNX`) | login |
| Order | `order:<order_id>` | HASH | – | `checkout.lua`, worker | orders API |
| User's orders | `orders:<user_id>` | ZSET (score = created ms) | – | `checkout.lua` | `GET /orders` |
| Cart audit log | `stream:cart-events` | STREAM (capped ~100 k) | – | scripts | admin events API |
| Order queue | `stream:orders` + group `order-workers` | STREAM + consumer group | – | `checkout.lua` | order worker |
| Instance heartbeat | `instance:<instance_id>` | STRING (JSON) | 6 s | each instance every 2 s | `/admin/cluster` |
| Failure injection | `chaos:<instance_id>` / `chaos:all` | STRING (JSON) | set by admin | admin API | instances |
| Cluster counters | `metrics:cluster` | HASH | – | API, worker | `/admin/cluster` |
| Invalidation channel | `cart-invalidate`, `product-invalidate` | PUB/SUB | – | scripts / admin | every instance |

## Cart — `cart:{u_fd36fce56af6}` (HASH)

| Field | Type | Meaning |
|---|---|---|
| `v` | integer | **version**, +1 on every successful change, never decreases (not even on checkout/clear). Exposed as `ETag`. |
| `item:<sku>` | integer | quantity of that product (field absent = not in cart) |
| `updated_at` | epoch ms | last change |
| `updated_by` | string | instance that performed the last change (traceability) |

Sample (real record from the demo run):
```
v              8
updated_at     1791798311358
updated_by     app-3
item:SKU-CAP   4
TTL            604763 s   (≈ 7 days; renewed on every write → abandoned carts expire automatically)
```
**Why a hash with one field per line, not one JSON blob?** Two devices adding *different* products are
independent field updates, and adding to the *same* product is a single atomic `HINCRBY`. Nothing is
"read the whole cart, change it, write it all back", which is the pattern that loses updates (see E1 in the report).

## Idempotency record — `idem:{u_…}:<Idempotency-Key>` (STRING)

Value = `<request fingerprint>|<JSON result returned the first time>`:
```
dd12db687a762876|{"order_id":"o_48ece3c3c8f7","total_cents":599400,"status":"PENDING","cart_version":4,"items":[...]}
TTL 86361 s
```
* same key + same fingerprint → the stored result is replayed (no second change)
* same key + different fingerprint → `422 IDEMPOTENCY_KEY_REUSED`
* written **in the same atomic script** as the change itself, so "change applied" and "result remembered" can never diverge, even if the instance dies right after.

## Product — `product:SKU-TSHIRT` (HASH)
```
sku          SKU-TSHIRT
name         Cotton T-Shirt
price_cents  49900          (integer paise/cents — no floating-point money)
stock        99995          (decremented atomically by checkout.lua; can never go below 0)
```

## User — `user:u_fd36fce56af6` (HASH) + `users:by_name`
```
id             u_fd36fce56af6
username       alice6460
role           customer          (customer | admin | service)
password_hash  scrypt$<salt hex>$<derived key hex>   (scrypt n=2^14, r=8, p=1)
created_at     1791798308703

users:by_name  alice6460 -> u_fd36fce56af6   (HSETNX = unique usernames without races)
```

## Order — `order:o_48ece3c3c8f7` (HASH) + `orders:<user>` (ZSET)
```
id            o_48ece3c3c8f7
user          u_fd36fce56af6
status        CONFIRMED                 (PENDING → CONFIRMED by the worker)
items         [{"sku":"SKU-SNEAKER","qty":1,"name":"Running Sneakers","price_cents":349900},
               {"sku":"SKU-TSHIRT","qty":5,"name":"Cotton T-Shirt","price_cents":49900}]
total_cents   599400
cart_version  3                         (exact cart version the order was built from)
created_at    1791798308989   created_by   app-2
confirmed_at  1791798309012   confirmed_by worker-1

orders:u_fd36fce56af6   ZSET  o_48ece3c3c8f7 -> 1791798308989
```
Prices are copied into the order (a later price change does not alter past orders).

## Streams

`stream:cart-events`: one entry per cart change (audit log / change feed):
```
1791798273172-0  user u_864066b0de3b op set sku SKU-TSHIRT qty 5 version 3 instance app-1 ts 1791798273171
1791798273082-0  user u_864066b0de3b op add sku SKU-SNEAKER qty 1 version 2 instance app-2 ts 1791798273082
```

`stream:orders`: work queue with consumer group `order-workers`:
```
1791798311271-0  order_id o_7e18393a489c  user u_19525fcc71a8
XINFO GROUPS: name order-workers, consumers 1, pending 0, lag 0
```

## Relationships

```
user 1 ── 1 cart (cart:{user_id})           user 1 ── * orders (orders:<user_id> → order:<id>)
cart * ── * product (item:<sku> fields)     order * ── * product (items snapshot)
cart 1 ── * cart-events (by user field)     order 1 ── 1 orders-stream entry
```

## Consistency rules enforced by the schema

| Rule | Mechanism |
|---|---|
| no lost updates | all cart writes inside one Lua script; relative changes use `HINCRBY` |
| no blind overwrites | absolute writes (`PUT`, checkout) require `If-Match` = `v` |
| no duplicates on retry | `idem:` record written atomically with the change |
| no overselling | stock checked for *all* lines, then decremented, in one script |
| versions only grow | `v` is never reset (clear/checkout `HDEL` items, `HINCRBY v`) |
| stale caches detected | every change publishes `user_id:version`; instances evict older entries; clients can pass `X-Min-Version` |
| abandoned carts expire | `PEXPIRE` 7 days on every write |
| bounded storage | streams capped with `MAXLEN ~ 100000`; idempotency records expire after 24 h; `maxmemory-policy noeviction` so data is never silently dropped |
