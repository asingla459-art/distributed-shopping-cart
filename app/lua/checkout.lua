-- Atomic checkout: version check + stock reservation + order creation + cart reset.
-- Either everything happens or nothing does, so stock can never go negative
-- (no overselling) and an order is never created from a stale cart.
--
-- KEYS[1] cart hash, KEYS[2] idempotency key ("" if none), KEYS[3] orders stream,
-- KEYS[4] cart events stream, KEYS[5] order hash, KEYS[6] user's order index,
-- KEYS[7..] product hashes for every sku currently in the cart
-- ARGV: expected_version, order_id, user_id, now_ms, instance, fingerprint,
--       idem_ttl, cart_ttl, then the skus (same order as KEYS[7..])
local cart, idem, ostream, events, order, uindex = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local expected, order_id, user_id, now = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local instance, fp, idem_ttl, cart_ttl = ARGV[5], ARGV[6], tonumber(ARGV[7]), tonumber(ARGV[8])

if idem ~= '' then
  local prev = redis.call('GET', idem)
  if prev then
    local sep = string.find(prev, '|', 1, true)
    if string.sub(prev, 1, sep - 1) ~= fp then return {'IDEM_MISMATCH', ''} end
    return {'REPLAY', string.sub(prev, sep + 1)}
  end
end

local current = tonumber(redis.call('HGET', cart, 'v') or '0')
if tonumber(expected) ~= current then return {'CONFLICT', tostring(current)} end

-- collect cart items
local flat = redis.call('HGETALL', cart)
local items = {}
local n = 0
for i = 1, #flat, 2 do
  if string.sub(flat[i], 1, 5) == 'item:' then
    items[string.sub(flat[i], 6)] = tonumber(flat[i + 1]); n = n + 1
  end
end
if n == 0 then return {'EMPTY', ''} end

-- the caller passed product keys for the cart it saw; must match exactly
local passed = #KEYS - 6
if passed ~= n then return {'CONFLICT', tostring(current)} end

-- check stock for every line first (no partial reservation)
local lines, total = {}, 0
for i = 7, #KEYS do
  local sku = ARGV[i + 2]
  local q = items[sku]
  if not q then return {'CONFLICT', tostring(current)} end
  local p = redis.call('HMGET', KEYS[i], 'stock', 'price_cents', 'name')
  if not p[1] then return {'NO_PRODUCT', sku} end
  if tonumber(p[1]) < q then return {'OUT_OF_STOCK', sku .. ':' .. p[1] .. ':' .. q} end
  table.insert(lines, {sku = sku, qty = q, price_cents = tonumber(p[2]), name = p[3]})
  total = total + q * tonumber(p[2])
end
-- reserve stock
for i = 7, #KEYS do
  redis.call('HINCRBY', KEYS[i], 'stock', -items[ARGV[i + 2]])
end

local lines_json = cjson.encode(lines)
redis.call('HSET', order, 'id', order_id, 'user', user_id, 'status', 'PENDING',
  'items', lines_json, 'total_cents', total, 'cart_version', current,
  'created_at', now, 'created_by', instance)
redis.call('ZADD', uindex, now, order_id)
redis.call('XADD', ostream, 'MAXLEN', '~', 100000, '*', 'order_id', order_id, 'user', user_id)

-- empty the cart but keep the version counter monotonic
for sku, _ in pairs(items) do redis.call('HDEL', cart, 'item:' .. sku) end
local v = redis.call('HINCRBY', cart, 'v', 1)
redis.call('HSET', cart, 'updated_at', now, 'updated_by', instance)
redis.call('PEXPIRE', cart, cart_ttl * 1000)
redis.call('XADD', events, 'MAXLEN', '~', 100000, '*', 'user', user_id, 'op', 'checkout',
  'sku', '', 'qty', '0', 'version', v, 'instance', instance, 'ts', now, 'order_id', order_id)
redis.call('PUBLISH', 'cart-invalidate', user_id .. ':' .. v)

local res = cjson.encode({order_id = order_id, status = 'PENDING', total_cents = total,
  items = cjson.decode(lines_json), cart_version = v})
if idem ~= '' then redis.call('SET', idem, fp .. '|' .. res, 'EX', idem_ttl) end
return {'OK', res}
