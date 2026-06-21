from __future__ import annotations

"""
This is the heart of the quota-meter engine.
Handles all Redis operations including atomic Lua scripts.

Key ideas:
- Quota deduction is atomic.
- Idempotency is handled inside the same Lua script as quota mutation.
- Refunds are tied to the original successful consume request.
"""


from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis

COUNTER_TTL_SECONDS = 35 * 24 * 60 * 60  # 35 days
REQUEST_TTL_SECONDS = 7 * 24 * 60 * 60  # FIX (Flaw B): Keep request records for 7 days only to prevent OOM


CONSUME_LUA_SCRIPT = """
-- KEYS[1] = counter_key
-- KEYS[2] = config_key
-- KEYS[3] = request_key
--
-- ARGV[1] = units
-- ARGV[2] = org_id
-- ARGV[3] = feature
-- ARGV[4] = period
-- ARGV[5] = request_ttl_seconds
-- ARGV[6] = counter_ttl_seconds

-- Request record format:
-- {
--   "org_id": "...",
--   "feature": "...",
--   "units": 10,
--   "granted": true/false,
--   "remaining": 490,
--   "period": "2026-06",
--   "refunded": false
-- }

-- 1) Idempotency check: if request already exists, return prior result.
local existing = redis.call('GET', KEYS[3])
if existing then
    local obj = cjson.decode(existing)
    local granted_num = obj.granted and 1 or 0
    local refunded_num = obj.refunded and 1 or 0
    return {2, granted_num, tonumber(obj.remaining), refunded_num}
end

-- 2) Load configured limit
local limit = tonumber(redis.call('GET', KEYS[2]))
if not limit then
    return {-1, 0, 0, 0} -- org not configured
end

-- 3) Load current usage
local current = tonumber(redis.call('GET', KEYS[1]) or 0)
local units = tonumber(ARGV[1])
local remaining = limit - current

local granted = 0
local new_remaining = remaining

if remaining >= units then
    redis.call('INCRBY', KEYS[1], units)
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[6]))
    granted = 1
    new_remaining = remaining - units
else
    granted = 0
    new_remaining = remaining
end

local request_record = cjson.encode({
    org_id = ARGV[2],
    feature = ARGV[3],
    units = units,
    granted = granted == 1,
    remaining = new_remaining,
    period = ARGV[4],
    refunded = false
})

redis.call('SET', KEYS[3], request_record, 'EX', tonumber(ARGV[5]))

return {1, granted, new_remaining, 0}
"""


REFUND_LUA_SCRIPT = """
-- KEYS[1] = counter_key
-- KEYS[2] = config_key
-- KEYS[3] = request_key
--
-- ARGV[1] = org_id
-- ARGV[2] = feature
-- ARGV[3] = request_ttl_seconds

-- Refunds the ORIGINAL consume request, not an arbitrary unit amount.

local request_raw = redis.call('GET', KEYS[3])
if not request_raw then
    return {-2, 0} -- original request not found
end

local obj = cjson.decode(request_raw)

if obj.org_id ~= ARGV[1] or obj.feature ~= ARGV[2] then
    return {-3, 0} -- request does not belong to supplied org/feature
end

if not obj.granted then
    return {-4, tonumber(obj.remaining)} -- cannot refund a denied request
end

if obj.refunded then
    return {2, tonumber(obj.remaining)} -- already refunded (idempotent success)
end

local limit = tonumber(redis.call('GET', KEYS[2]))
if not limit then
    return {-1, 0} -- org not configured
end

local current = tonumber(redis.call('GET', KEYS[1]) or 0)
local units = tonumber(obj.units)

local new_value = current - units
if new_value < 0 then
    new_value = 0
end

-- FIX (Flaw C): Use KEEPTTL flag to prevent clearing the counter's monthly expiration window
redis.call('SET', KEYS[1], new_value, 'KEEPTTL')
local new_remaining = limit - new_value

obj.refunded = true
obj.remaining = new_remaining
redis.call('SET', KEYS[3], cjson.encode(obj), 'EX', tonumber(ARGV[3]))

return {1, new_remaining}
"""


def _get_period() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


# FIX (Flaw A): Wrap org_id in curly braces to ensure all keys hash to the same cluster slot
def _counter_key(org_id: str, feature: str, period: str) -> str:
    return f"quota:{{{org_id}}}:{feature}:{period}"


# FIX (Flaw A): Wrap org_id in curly braces to ensure all keys hash to the same cluster slot
def _config_key(org_id: str, feature: str) -> str:
    return f"quota_config:{{{org_id}}}:{feature}"


# FIX (Flaw A): Added org_id parameter and wrapped in curly braces to solve CROSSSLOT cluster errors
def _request_key(org_id: str, idempotency_key: str) -> str:
    return f"quota_request:{{{org_id}}}:{idempotency_key}"


async def atomic_consume(
    redis: Redis,
    org_id: str,
    feature: str,
    units: int,
    idempotency_key: str,
) -> dict[str, Any]:
    period = _get_period()
    counter_key = _counter_key(org_id, feature, period)
    config_key = _config_key(org_id, feature)
    request_key = _request_key(org_id, idempotency_key)  # Passed org_id here

    result = await redis.eval(
        CONSUME_LUA_SCRIPT,
        3,
        counter_key,
        config_key,
        request_key,
        units,
        org_id,
        feature,
        period,
        REQUEST_TTL_SECONDS,
        COUNTER_TTL_SECONDS,
    )

    status = int(result[0])

    if status == -1:
        raise ValueError("org_not_configured")

    replayed = status == 2
    granted = bool(int(result[1]))
    remaining = int(result[2])
    refunded = bool(int(result[3]))

    return {
        "granted": granted,
        "remaining": remaining,
        "org_id": org_id,
        "feature": feature,
        "period": period,
        "refunded": refunded,
        "replayed": replayed,
    }


async def atomic_refund_by_request(
    redis: Redis,
    org_id: str,
    feature: str,
    original_idempotency_key: str,
) -> dict[str, Any]:
    period = _get_period()
    counter_key = _counter_key(org_id, feature, period)
    config_key = _config_key(org_id, feature)
    request_key = _request_key(org_id, original_idempotency_key)  # Passed org_id here

    result = await redis.eval(
        REFUND_LUA_SCRIPT,
        3,
        counter_key,
        config_key,
        request_key,
        org_id,
        feature,
        REQUEST_TTL_SECONDS,
    )

    status = int(result[0])
    remaining = int(result[1])

    if status == -1:
        raise ValueError("org_not_configured")
    if status == -2:
        raise ValueError("original_request_not_found")
    if status == -3:
        raise ValueError("request_org_feature_mismatch")
    if status == -4:
        raise ValueError("cannot_refund_denied_request")

    return {
        "granted": True,
        "remaining": remaining,
        "org_id": org_id,
        "feature": feature,
        "period": period,
        "already_refunded": status == 2,
    }


async def get_usage(
    redis: Redis,
    org_id: str,
    feature: str,
) -> tuple[int, int, int, str]:
    period = _get_period()
    counter_key = _counter_key(org_id, feature, period)
    config_key = _config_key(org_id, feature)

    limit = await redis.get(config_key)
    current = await redis.get(counter_key)

    if limit is None:
        raise ValueError("org_not_configured")

    limit_int = int(limit)
    current_int = int(current or 0)
    return limit_int, current_int, limit_int - current_int, period
