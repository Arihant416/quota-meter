"""
This is the heart of the quota-meter engine.
Handles all Redis operations including atomic Lua scripts.
"""

from redis.asyncio import Redis
from datetime import datetime, timezone

DEDUCT_LUA_SCRIPT = """
-- KEYS[1] = counter_key  (quota:org1:container-tracking:2026-06)
-- KEYS[2] = config_key   (quota_config:org1:container-tracking)
-- ARGV[1] = units requested

-- step 1: get limit, if nil org not configured
local limit = tonumber(redis.call('GET', KEYS[2]))
if not limit then
    return {-1, 0}   -- -1 = org not configured → 403
end

-- step 2: get current counter, nil means first request of month
local current = tonumber(redis.call('GET', KEYS[1]) or 0)

-- step 3: calculate remaining
local remaining = limit - current

-- step 4: check if enough quota
if remaining >= tonumber(ARGV[1]) then
    redis.call('INCRBY', KEYS[1], ARGV[1])
    redis.call('EXPIRE', KEYS[1], 3024000)  -- 35 days in seconds
    return {1, remaining - tonumber(ARGV[1])}  -- granted
else
    return {0, remaining}  -- denied
end
"""

REFUND_LUA_SCRIPT = """
-- KEYS[1] = counter_key
-- KEYS[2] = config_key
-- ARGV[1] = units to refund

local limit = tonumber(redis.call('GET', KEYS[2]))
if not limit then
    return {-1, 0}  -- org not configured
end

local current = tonumber(redis.call('GET', KEYS[1]) or 0)

-- don't go below 0, don't go above limit
local new_value = math.max(0, current - tonumber(ARGV[1]))
redis.call('SET', KEYS[1], new_value)

return {1, limit - new_value}  -- granted, new remaining
"""


def _get_period() -> str:
    """Returns current UTC period in YYYY-MM format."""
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _get_keys(org_id: str, feature: str) -> tuple[str, str]:
    """Returns (counter_key, config_key) for given org and feature."""
    period = _get_period()
    counter_key = f"quota:{org_id}:{feature}:{period}"
    config_key = f"quota_config:{org_id}:{feature}"
    return counter_key, config_key


async def atomic_deduct(
    redis: Redis, org_id: str, feature: str, units: int
) -> tuple[int, int]:
    """
    Atomically checks and deducts quota units.
    Returns (status_code, remaining)
      -1 = org not configured → 403
       0 = quota exhausted   → 429
       1 = granted           → 200
    """
    counter_key, config_key = _get_keys(org_id, feature)
    result = await redis.eval(DEDUCT_LUA_SCRIPT, 2, counter_key, config_key, units)
    return int(result[0]), int(result[1])


async def atomic_refund(
    redis: Redis, org_id: str, feature: str, units: int
) -> tuple[int, int]:
    """
    Atomically refunds quota units back to the counter.
    Returns (status_code, remaining)
      -1 = org not configured → 403
       1 = refund successful  → 200
    """
    counter_key, config_key = _get_keys(org_id, feature)
    result = await redis.eval(REFUND_LUA_SCRIPT, 2, counter_key, config_key, units)
    return int(result[0]), int(result[1])


async def get_usage(
    redis: Redis, org_id: str, feature: str
) -> tuple[int, int, int, str]:
    """
    Fetches current usage from Redis.
    Returns (limit, used, remaining, period)
    Raises ValueError if org not configured.
    """
    counter_key, config_key = _get_keys(org_id, feature)

    limit = await redis.get(config_key)
    current = await redis.get(counter_key)

    if limit is None:
        raise ValueError("org_not_configured")

    limit = int(limit)
    current = int(current or 0)

    return limit, current, limit - current, _get_period()
