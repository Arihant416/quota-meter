"""
Business logic layer — orchestrates store calls,
handles idempotency, and builds response models.
"""

import json
import calendar
from datetime import datetime, timezone
from redis.asyncio import Redis
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.quota.models import QuotaResult, UsageResponse
from app.quota import store


def _get_resets_at() -> datetime:
    """Returns the last moment of the current month in UTC."""
    now = datetime.now(timezone.utc)
    last_day = calendar.monthrange(now.year, now.month)[1]
    return datetime(now.year, now.month, last_day, 23, 59, 59, tzinfo=timezone.utc)


def _get_period() -> str:
    """Returns current UTC period in YYYY-MM format."""
    return datetime.now(timezone.utc).strftime("%Y-%m")


async def consume(
    redis: Redis,
    db: AsyncIOMotorDatabase,
    org_id: str,
    feature: str,
    units: int,
    idempotency_key: str,
) -> QuotaResult:
    """
    Checks and deducts quota atomically.
    Idempotent — same key returns cached result.
    """
    # step 1 — check idempotency cache
    idem_key = f"idempotency:{idempotency_key}"
    cached = await redis.get(idem_key)
    if cached:
        return QuotaResult.model_validate_json(cached)

    # step 2 — atomic deduct via Lua script
    status, remaining = await store.atomic_deduct(redis, org_id, feature, units)

    # step 3 — build result based on status code
    if status == -1:
        raise ValueError("org_not_configured")

    result = QuotaResult(
        granted=status == 1,
        remaining=remaining,
        resets_at=_get_resets_at(),
        org_id=org_id,
        feature=feature,
    )

    # step 4 — cache result against idempotency key (TTL 24h)
    await redis.set(idem_key, result.model_dump_json(), ex=86400)

    return result


async def refund(
    redis: Redis,
    org_id: str,
    feature: str,
    units: int,
    idempotency_key: str,
) -> QuotaResult:
    """
    Refunds quota units back to the counter.
    Idempotent — same key returns cached result.
    """
    # step 1 — check idempotency cache
    idem_key = f"idempotency:refund:{idempotency_key}"
    cached = await redis.get(idem_key)
    if cached:
        return QuotaResult.model_validate_json(cached)

    # step 2 — atomic refund via Lua script
    status, remaining = await store.atomic_refund(redis, org_id, feature, units)

    # step 3 — build result
    if status == -1:
        raise ValueError("org_not_configured")

    result = QuotaResult(
        granted=True,
        remaining=remaining,
        resets_at=_get_resets_at(),
        org_id=org_id,
        feature=feature,
    )

    # step 4 — cache refund result (TTL 24h)
    await redis.set(idem_key, result.model_dump_json(), ex=86400)

    return result


async def get_usage(
    redis: Redis,
    db: AsyncIOMotorDatabase,
    org_id: str,
    feature: str,
) -> UsageResponse:
    """
    Returns current quota usage for an org and feature.
    """
    limit, used, remaining, period = await store.get_usage(redis, org_id, feature)

    return UsageResponse(
        org_id=org_id,
        feature=feature,
        period=period,
        limit=limit,
        used=used,
        remaining=remaining,
        resets_at=_get_resets_at(),
    )
