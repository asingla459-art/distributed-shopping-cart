#!/usr/bin/env bash
# Mandatory demonstration script (viva): API execution, storage state, logs/metrics.
#   deploy/local/cluster.sh start consistent 3      # or: docker compose up -d
#   experiments/demo.sh [section]   sections: api conflict redis failure oversell metrics logs   (default: all)
# Set BASE (default http://localhost:8080) and RCLI (default "redis-cli") for a cloud deployment, e.g.
#   BASE=http://<vm-ip> RCLI="docker compose exec -T redis redis-cli -a $REDIS_PASSWORD --no-auth-warning" experiments/demo.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BASE=${BASE:-http://localhost:8080}
API=$BASE/api/v1
RCLI=${RCLI:-redis-cli}
STATE=${STATE_FILE:-run/demo.env}
mkdir -p run
step() { printf '\n\033[1;36m### %s\033[0m\n' "$*"; }
show() { printf '\033[0;33m$ %s\033[0m\n' "$*"; }
req() {  # req METHOD PATH [json] [extra curl args...]  -> prints status, key headers, body
  local m=$1 p=$2 d=${3:-}; shift 3 2>/dev/null || shift $#
  show "curl -X $m $p ${d:+-d '$d'} $*"
  local out; out=$(curl -s -i -X "$m" "$API$p" -H "Authorization: Bearer ${TOKEN:-}" ${d:+-d "$d"} "$@")
  echo "$out" | grep -i -E '^HTTP|^etag|^x-instance|^idempotent-replayed|^x-upstream-tries|^location' | tr -d '\r'
  echo "$out" | tail -1 | jq -c . 2>/dev/null || echo "$out" | tail -1
  echo
}
etag() { curl -s -D - -o /dev/null "$API/cart" -H "Authorization: Bearer $TOKEN" | grep -i etag | tr -d '\r' | awk '{print $2}'; }

login_alice() {
  local u=alice$((RANDOM % 9000 + 1000))
  local r; r=$(curl -s -X POST "$API/auth/register" -d "{\"username\":\"$u\",\"password\":\"alice-pass-1\"}")
  TOKEN=$(echo "$r" | jq -r .token); UID_=$(echo "$r" | jq -r .id)
  ADMIN=$(curl -s -X POST "$API/auth/login" -d '{"username":"admin","password":"admin-pass-123"}' | jq -r .token)
  printf 'TOKEN=%s\nUID_=%s\nADMIN=%s\n' "$TOKEN" "$UID_" "$ADMIN" > "$STATE"
}
[ -f "$STATE" ] && source "$STATE"

sec_api() {
  step "1. Requests are spread over 3 stateless instances by the load balancer"
  for i in 1 2 3; do curl -s -D - -o /dev/null "$BASE/health" | grep -i x-instance | tr -d '\r'; done
  step "2. Register a customer (JWT issued, role=customer)"
  login_alice
  echo "user id: $UID_   token: ${TOKEN:0:40}..."
  step "3. Add to cart with an Idempotency-Key"
  req POST /cart/items '{"sku":"SKU-TSHIRT","qty":2}' -H "Idempotency-Key: demo-add-0001"
  step "4. Client times out and RETRIES the same request -> replayed, not applied twice"
  req POST /cart/items '{"sku":"SKU-TSHIRT","qty":2}' -H "Idempotency-Key: demo-add-0001"
  step "5. Add another product, read the cart (ETag = version)"
  req POST /cart/items '{"sku":"SKU-SNEAKER","qty":1}' -H "Idempotency-Key: demo-add-0002"
  req GET /cart ""
  step "6. RBAC: a customer cannot call admin APIs"
  req GET /admin/cluster ""
}

sec_conflict() {
  step "7. Two devices edit the same line concurrently (both read version $(etag))"
  V=$(etag)
  echo "phone sets qty=5 with If-Match: $V"
  req PUT /cart/items/SKU-TSHIRT '{"qty":5}' -H "If-Match: $V"
  echo "laptop sets qty=9 with the SAME stale If-Match: $V"
  req PUT /cart/items/SKU-TSHIRT '{"qty":9}' -H "If-Match: $V"
  echo "-> 409 VERSION_CONFLICT with the current cart; the laptop re-reads and retries with the new ETag"
  step "8. Absolute updates without If-Match are refused (428)"
  req PUT /cart/items/SKU-TSHIRT '{"qty":9}'
}

sec_redis() {
  step "9. Storage state in Redis for this cart"
  show "$RCLI HGETALL cart:{$UID_}"; $RCLI HGETALL "cart:{$UID_}" | paste - -
  show "$RCLI TTL cart:{$UID_}"; $RCLI TTL "cart:{$UID_}"
  show "$RCLI GET idem:{$UID_}:demo-add-0001"; $RCLI GET "idem:{$UID_}:demo-add-0001"
  show "$RCLI XREVRANGE stream:cart-events + - COUNT 3"
  $RCLI XREVRANGE stream:cart-events + - COUNT 3 | paste -sd' ' | sed 's/ \([0-9]\{13\}-[0-9]\)/\n\1/g'
  echo
}

sec_oversell() {
  step "10. Checkout: version check + stock reservation + order creation in one atomic script"
  V=$(etag)
  req POST /cart/checkout "" -H "If-Match: $V" -H "Idempotency-Key: demo-checkout-01"
  sleep 1
  OID=$(curl -s "$API/orders" -H "Authorization: Bearer $TOKEN" | jq -r '.orders[0].id')
  req GET "/orders/$OID" ""
  echo "-> status CONFIRMED was set asynchronously by the order worker (Redis Streams consumer group)"
  step "11. Oversell protection: 20 buyers race for 5 'Limited Edition Hoodie' units"
  TOKEN=$ADMIN req POST /products '{"sku":"SKU-LIMITED","name":"Limited Edition Hoodie","price_cents":259900,"stock":5}'
  python3 - "$BASE" <<'PY'
import asyncio, sys, uuid
from urllib.parse import urlparse
sys.path.insert(0, ".")
from loadtest.httpclient import new_user
u = urlparse(sys.argv[1])
async def main():
    users = [await new_user(f"buyer{uuid.uuid4().hex[:6]}_", i, u.hostname, u.port or 80) for i in range(20)]
    v = {}
    for x in users:
        r = await x.request("POST", "/api/v1/cart/items", {"sku": "SKU-LIMITED", "qty": 1},
                            {"Idempotency-Key": uuid.uuid4().hex})
        v[x.user_id] = r.json()["version"]
    res = await asyncio.gather(*(x.request("POST", "/api/v1/cart/checkout", None,
          {"If-Match": f'"{v[x.user_id]}"', "Idempotency-Key": uuid.uuid4().hex}) for x in users))
    codes = [r.status for r in res]
    print(f"201 Created: {codes.count(201)}   409 OUT_OF_STOCK: {codes.count(409)}")
    print("example rejection:", next(r.json() for r in res if r.status == 409))
asyncio.run(main())
PY
  show "$RCLI HGET product:SKU-LIMITED stock"; $RCLI HGET product:SKU-LIMITED stock
}

sec_failure() {
  step "12. Controlled failure: kill app-2 while requests are flowing"
  # crash app-2 through the admin chaos API (process exits -> Docker / the local supervisor restarts it)
  TOKEN=$ADMIN req POST /admin/chaos '{"instance":"app-2","mode":"crash_now","ttl_seconds":10}'
  sleep 2.5
  for i in 1 2 3 4; do
    curl -s -D - -o /dev/null -X POST "$API/cart/items" -H "Authorization: Bearer $TOKEN" \
      -H "Idempotency-Key: demo-fail-$i-$RANDOM" -d '{"sku":"SKU-CAP","qty":1}' \
      | grep -i -E '^HTTP|^x-instance|^x-upstream-tries' | tr -d '\r' | paste -sd' '
  done
  echo "-> no request failed: the LB retried on a healthy instance (X-Upstream-Tries: 2) and marked app-2 down"
  sleep 3
  cat run/supervisor.log 2>/dev/null | tail -1
  TOKEN=$ADMIN req GET /admin/cluster ""
}

sec_metrics() {
  step "13. Metrics (Prometheus format) - scraped from every instance through the LB"
  for i in 1 2 3; do curl -s "$BASE/metrics"; done | grep -E "^cart_(version_conflicts|idempotent_replays|cache_hits|cache_misses|cache_invalidations)_total|orders_created_total|POST /api/v1/cart/items\",le=\"0.01\"|cart_http_requests_total.*cart/items.*status=\"200\"" | sort | head -14
  step "14. Audit events for this user (admin API)"
  TOKEN=$ADMIN req GET "/admin/events?count=4&user=$UID_" ""
}

sec_logs() {
  step "15. Structured logs: conflict, replay, crash, LB failover, restart"
  grep -h -E '"version_conflict"|"replay": true|chaos|instance_started|order_confirmed' run/app-*.jsonl 2>/dev/null | sort | tail -6
  grep -h upstream_failed run/lb.out | tail -2
  tail -2 run/supervisor.log 2>/dev/null
}

case "${1:-all}" in
  all) sec_api; sec_conflict; sec_redis; sec_oversell; sec_failure; sec_metrics; sec_logs;;
  *) "sec_$1";;
esac
