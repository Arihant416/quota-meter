"""
Business logic layer — orchestrates store calls,
handles idempotency, and builds response models.
"""


from __future__ import annotations

from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.quota import store
from app.quota.models import QuotaResult, UsageResponse


def _get_resets_at() -> datetime:
    now = datetime.now(timezone.utc)
    if now.month == 12:
        return datetime(now.year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return datetime(now.year, now.month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)


async def consume(
    redis: Redis,
    db: AsyncIOMotorDatabase | None,
    org_id: str,
    feature: str,
    units: int,
    idempotency_key: str,
) -> QuotaResult:
    """
    Atomically consumes quota with built-in idempotency.
    """
    result = await store.atomic_consume(
        redis=redis,
        org_id=org_id,
        feature=feature,
        units=units,
        idempotency_key=idempotency_key,
    )

    return QuotaResult(
        granted=result["granted"],
        remaining=result["remaining"],
        resets_at=_get_resets_at(),
        org_id=org_id,
        feature=feature,
    )


async def refund(
    redis: Redis,
    org_id: str,
    feature: str,
    original_idempotency_key: str,
) -> QuotaResult:
    """
    Refunds a previously granted consume request by its original idempotency key.
    """
    result = await store.atomic_refund_by_request(
        redis=redis,
        org_id=org_id,
        feature=feature,
        original_idempotency_key=original_idempotency_key,
    )

    return QuotaResult(
        granted=True,
        remaining=result["remaining"],
        resets_at=_get_resets_at(),
        org_id=org_id,
        feature=feature,
    )


async def get_usage(
    redis: Redis,
    db: AsyncIOMotorDatabase | None,
    org_id: str,
    feature: str,
) -> UsageResponse:
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
