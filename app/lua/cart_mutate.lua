-- Atomic, versioned, idempotent cart mutation.
-- Runs inside Redis as ONE indivisible step, so two app instances can never
-- interleave a read and a write on the same cart (no lost updates).
--
-- KEYS[1] cart hash          cart:{<user_id>}
-- KEYS[2] idempotency key    idem:{<user_id>}:<key>   ("" when the client sent none)
-- KEYS[3] audit/event stream stream:cart-events
-- KEYS[4] product hash       product:<sku>             ("" for 'clear')
-- ARGV: op, sku, qty, expected_version, cart_ttl, idem_ttl, now_ms,
--       instance, user_id, fingerprint, max_qty
local cart, idem, events, product = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local op, sku, qty = ARGV[1], ARGV[2], tonumber(ARGV[3])
local expected = ARGV[4]
local cart_ttl, idem_ttl = tonumber(ARGV[5]), tonumber(ARGV[6])
local now, instance, user_id = ARGV[7], ARGV[8], ARGV[9]
local fp, max_qty = ARGV[10], tonumber(ARGV[11])

local function state()
  local flat = redis.call('HGETALL', cart)
  local items, v, upd = {}, 0, ''
  for i = 1, #flat, 2 do
    local f, val = flat[i], flat[i + 1]
    if f == 'v' then v = tonumber(val)
    elseif f == 'updated_at' then upd = val
    elseif string.sub(f, 1, 5) == 'item:' then items[string.sub(f, 6)] = tonumber(val) end
  end
  if next(items) == nil then items = cjson.decode('{}') end
  return cjson.encode({version = v, items = items, updated_at = upd})
end

-- 1. Duplicate request? Return the stored result instead of applying it twice.
if idem ~= '' then
  local prev = redis.call('GET', idem)
  if prev then
    local sep = string.find(prev, '|', 1, true)
    if string.sub(prev, 1, sep - 1) ~= fp then return {'IDEM_MISMATCH', ''} end
    return {'REPLAY', string.sub(prev, sep + 1)}
  end
end

-- 2. Optimistic concurrency: If-Match version must equal the stored version.
local current = tonumber(redis.call('HGET', cart, 'v') or '0')
if expected ~= '' and tonumber(expected) ~= current then
  return {'CONFLICT', state()}
end

-- 3. Apply the operation.
local field = 'item:' .. sku
if op == 'add' then
  if redis.call('EXISTS', product) == 0 then return {'NO_PRODUCT', ''} end
  local newq = redis.call('HINCRBY', cart, field, qty)
  if newq > max_qty then
    redis.call('HINCRBY', cart, field, -qty)
    return {'TOO_MANY', state()}
  end
  if newq <= 0 then redis.call('HDEL', cart, field) end
elseif op == 'set' then
  if redis.call('EXISTS', product) == 0 then return {'NO_PRODUCT', ''} end
  if qty > max_qty then return {'TOO_MANY', state()} end
  if qty <= 0 then redis.call('HDEL', cart, field) else redis.call('HSET', cart, field, qty) end
elseif op == 'remove' then
  if redis.call('HDEL', cart, field) == 0 then return {'NO_ITEM', state()} end
elseif op == 'clear' then
  local flat = redis.call('HKEYS', cart)
  for _, f in ipairs(flat) do
    if string.sub(f, 1, 5) == 'item:' then redis.call('HDEL', cart, f) end
  end
else
  return {'BAD_OP', ''}
end

-- 4. New version, sliding expiry, audit event, cross-instance invalidation.
local v = redis.call('HINCRBY', cart, 'v', 1)
redis.call('HSET', cart, 'updated_at', now, 'updated_by', instance)
redis.call('PEXPIRE', cart, cart_ttl * 1000)
redis.call('XADD', events, 'MAXLEN', '~', 100000, '*',
  'user', user_id, 'op', op, 'sku', sku, 'qty', tostring(qty or 0),
  'version', v, 'instance', instance, 'ts', now)
redis.call('PUBLISH', 'cart-invalidate', user_id .. ':' .. v)

local s = state()
if idem ~= '' then redis.call('SET', idem, fp .. '|' .. s, 'EX', idem_ttl) end
return {'OK', s}
