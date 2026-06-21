"""
Generated Using Claude.
Concurrency test — proves quota is never over-served.
This is the critical test. Runs without a live server,
tests the quota engine logic directly against a real Redis instance.

Run with:
    pytest tests/test_concurrency.py -v
"""
import asyncio
import uuid
from datetime import datetime, timezone

import pytest
from redis.asyncio import Redis

from app.core.config import REDIS_URL
from app.quota import service

TEST_ORG = "test-org"
TEST_FEATURE = "container-tracking"
QUOTA_LIMIT = 500


def current_period() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def counter_key(org_id: str = TEST_ORG, feature: str = TEST_FEATURE) -> str:
    # FIX: Wrap org_id in curly braces to align with cluster hash tag constraints
    return f"quota:{{{org_id}}}:{feature}:{current_period()}"


def config_key(org_id: str = TEST_ORG, feature: str = TEST_FEATURE) -> str:
    # FIX: Wrap org_id in curly braces to align with cluster hash tag constraints
    return f"quota_config:{{{org_id}}}:{feature}"


@pytest.fixture
async def redis():
    client = Redis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture(autouse=True)
async def clean_redis(redis: Redis):
    # clear current test org keys
    keys = await redis.keys("quota:*")
    if keys:
        await redis.delete(*keys)

    config_keys = await redis.keys("quota_config:*")
    if config_keys:
        await redis.delete(*config_keys)

    request_keys = await redis.keys("quota_request:*")
    if request_keys:
        await redis.delete(*request_keys)

    yield

    keys = await redis.keys("quota:*")
    if keys:
        await redis.delete(*keys)

    config_keys = await redis.keys("quota_config:*")
    if config_keys:
        await redis.delete(*config_keys)

    request_keys = await redis.keys("quota_request:*")
    if request_keys:
        await redis.delete(*request_keys)


async def setup_quota(redis: Redis, limit: int):
    await redis.set(config_key(), limit)


async def consume_once(redis: Redis, units: int):
    return await service.consume(
        redis=redis,
        db=None,
        org_id=TEST_ORG,
        feature=TEST_FEATURE,
        units=units,
        idempotency_key=str(uuid.uuid4()),
    )


@pytest.mark.asyncio
async def test_concurrent_over_limit_exactly_caps_at_quota(redis: Redis):
    """
    100 concurrent requests x 10 units = 1000 attempted against a quota of 500.
    Exactly 50 should be granted, 50 denied, final counter must be 500.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    tasks = [asyncio.create_task(consume_once(redis, 10)) for _ in range(100)]
    results = await asyncio.gather(*tasks)

    granted = [r for r in results if r.granted]
    denied = [r for r in results if not r.granted]

    assert len(granted) == 50, f"Expected 50 granted, got {len(granted)}"
    assert len(denied) == 50, f"Expected 50 denied, got {len(denied)}"

    used = int(await redis.get(counter_key()) or 0)
    assert used == 500, f"Expected final used counter 500, got {used}"


@pytest.mark.asyncio
async def test_exact_exhaustion(redis: Redis):
    """
    10 concurrent requests x 50 units = 500 total against quota 500.
    All should be granted and final counter should be exactly 500.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    tasks = [asyncio.create_task(consume_once(redis, 50)) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    assert all(r.granted for r in results)
    used = int(await redis.get(counter_key()) or 0)
    assert used == 500


@pytest.mark.asyncio
async def test_org_not_configured(redis: Redis):
    tasks = [
        asyncio.create_task(
            service.consume(
                redis=redis,
                db=None,
                org_id="unknown-org",
                feature=TEST_FEATURE,
                units=1,
                idempotency_key=str(uuid.uuid4()),
            )
        )
        for _ in range(10)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [r for r in results if isinstance(r, ValueError)]

    assert len(errors) == 10
    assert all(str(e) == "org_not_configured" for e in errors)


@pytest.mark.asyncio
async def test_same_idempotency_key_only_consumes_once(redis: Redis):
    """
    Concurrent retries with the same idempotency key must not double-charge.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    idem = str(uuid.uuid4())

    tasks = [
        asyncio.create_task(
            service.consume(
                redis=redis,
                db=None,
                org_id=TEST_ORG,
                feature=TEST_FEATURE,
                units=10,
                idempotency_key=idem,
            )
        )
        for _ in range(10)
    ]

    results = await asyncio.gather(*tasks)

    assert all(r.granted == results[0].granted for r in results)
    assert all(r.remaining == results[0].remaining for r in results)

    used = int(await redis.get(counter_key()) or 0)
    assert used == 10, f"Expected only 10 units consumed, got {used}"


@pytest.mark.asyncio
async def test_refund_by_original_request_idempotency_key(redis: Redis):
    """
    Refund must restore exactly the units consumed by the original granted request.
    Repeated refund should be idempotent.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    original_idem = str(uuid.uuid4())

    consume_result = await service.consume(
        redis=redis,
        db=None,
        org_id=TEST_ORG,
        feature=TEST_FEATURE,
        units=25,
        idempotency_key=original_idem,
    )
    assert consume_result.granted is True

    used_after_consume = int(await redis.get(counter_key()) or 0)
    assert used_after_consume == 25

    refund_result = await service.refund(
        redis=redis,
        org_id=TEST_ORG,
        feature=TEST_FEATURE,
        original_idempotency_key=original_idem,
    )
    assert refund_result.granted is True

    used_after_refund = int(await redis.get(counter_key()) or 0)
    assert used_after_refund == 0

    # second refund should not change state
    refund_result_2 = await service.refund(
        redis=redis,
        org_id=TEST_ORG,
        feature=TEST_FEATURE,
        original_idempotency_key=original_idem,
    )
    assert refund_result_2.granted is True

    used_after_second_refund = int(await redis.get(counter_key()) or 0)
    assert used_after_second_refund == 0


@pytest.mark.asyncio
async def test_denied_request_cannot_be_refunded(redis: Redis):
    await setup_quota(redis, 5)

    original_idem = str(uuid.uuid4())

    consume_result = await service.consume(
        redis=redis,
        db=None,
        org_id=TEST_ORG,
        feature=TEST_FEATURE,
        units=10,
        idempotency_key=original_idem,
    )
    assert consume_result.granted is False

    with pytest.raises(ValueError, match="cannot_refund_denied_request"):
        await service.refund(
            redis=redis,
            org_id=TEST_ORG,
            feature=TEST_FEATURE,
            original_idempotency_key=original_idem,
        )
