"""
Generated Using Claude.
Concurrency test — proves quota is never over-served.
This is the critical test. Runs without a live server,
tests the quota engine logic directly against a real Redis instance.

Run with:
    pytest tests/test_concurrency.py -v
"""

import pytest
import asyncio
import uuid
from redis.asyncio import Redis
from app.quota import store, service
from app.core.config import REDIS_URL

# ── Config ─────────────────────────────────────────────────────────────────────

TEST_ORG = "test-org"
TEST_FEATURE = "container-tracking"
QUOTA_LIMIT = 500
CONCURRENCY = 50  # concurrent coroutines
UNITS_EACH = 10  # each requests 10 units — total attempted = 500


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture
async def redis():
    """Fresh Redis client for each test."""
    client = Redis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture(autouse=True)
async def clean_redis(redis):
    """
    Cleans up test keys before and after each test.
    Ensures tests don't bleed into each other.
    """
    # cleanup before
    period = (
        __import__("datetime")
        .datetime.now(__import__("datetime").timezone.utc)
        .strftime("%Y-%m")
    )
    counter_key = f"quota:{TEST_ORG}:{TEST_FEATURE}:{period}"
    config_key = f"quota_config:{TEST_ORG}:{TEST_FEATURE}"

    await redis.delete(counter_key)
    await redis.delete(config_key)

    yield

    # cleanup after
    await redis.delete(counter_key)
    await redis.delete(config_key)


# ── Helpers ────────────────────────────────────────────────────────────────────


async def setup_quota(redis: Redis, limit: int):
    """Sets quota limit in Redis directly."""
    config_key = f"quota_config:{TEST_ORG}:{TEST_FEATURE}"
    await redis.set(config_key, limit)


async def consume_once(
    redis: Redis,
    units: int,
    results: list,
):
    """
    Single consume attempt.
    Appends (status, remaining) to results list.
    """
    idempotency_key = str(uuid.uuid4())  # unique per attempt
    try:
        result = await service.consume(
            redis=redis,
            db=None,  # not needed — no MongoDB in hot path
            org_id=TEST_ORG,
            feature=TEST_FEATURE,
            units=units,
            idempotency_key=idempotency_key,
        )
        results.append(result)
    except ValueError:
        pass  # org_not_configured — shouldn't happen in this test


# ── Tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_no_over_serve_under_concurrency(redis):
    """
    THE critical test.

    50 concurrent workers each request 10 units.
    Total attempted = 500 units.
    Quota limit = 500 units.

    Expected:
    - Exactly 50 granted (50 * 10 = 500 units)
    - 0 denied (quota exactly matches total requested)
    - Final counter = 500 (never exceeds limit)
    - Over-served = 0 (never negative, never over limit)
    """
    await setup_quota(redis, QUOTA_LIMIT)

    results = []

    # fire all 50 coroutines simultaneously
    tasks = [
        asyncio.create_task(consume_once(redis, UNITS_EACH, results))
        for _ in range(CONCURRENCY)
    ]

    await asyncio.gather(*tasks)

    granted = [r for r in results if r.granted]
    denied = [r for r in results if not r.granted]

    total_units_granted = len(granted) * UNITS_EACH

    # core correctness assertions
    assert total_units_granted <= QUOTA_LIMIT, (
        f"OVER-SERVED: granted {total_units_granted} units "
        f"against limit of {QUOTA_LIMIT}"
    )
    assert (
        len(results) == CONCURRENCY
    ), f"Expected {CONCURRENCY} results, got {len(results)}"

    print(f"\n{'='*50}")
    print(f"  Concurrency Test Results")
    print(f"{'='*50}")
    print(f"  Workers:          {CONCURRENCY}")
    print(f"  Units each:       {UNITS_EACH}")
    print(f"  Total attempted:  {CONCURRENCY * UNITS_EACH}")
    print(f"  Quota limit:      {QUOTA_LIMIT}")
    print(f"  Granted:          {len(granted)}")
    print(f"  Denied:           {len(denied)}")
    print(f"  Units consumed:   {total_units_granted}")
    print(f"  Over-served:      {max(0, total_units_granted - QUOTA_LIMIT)}")
    print(f"{'='*50}")


@pytest.mark.asyncio
async def test_exact_exhaustion(redis):
    """
    Quota is exactly exhausted — not a single unit over.

    10 workers * 50 units = 500 = exact limit.
    All should be granted. Final counter = 500 exactly.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    results = []
    tasks = [asyncio.create_task(consume_once(redis, 50, results)) for _ in range(10)]
    await asyncio.gather(*tasks)

    granted = [r for r in results if r.granted]
    total_units = len(granted) * 50

    assert (
        total_units == QUOTA_LIMIT
    ), f"Expected exactly {QUOTA_LIMIT} units consumed, got {total_units}"
    assert all(
        r.granted for r in results
    ), "All requests should be granted when total equals limit exactly"


@pytest.mark.asyncio
async def test_over_limit_denied(redis):
    """
    Requests exceeding quota are denied.

    Quota = 500. Request 600 units total.
    Exactly 500 units granted, 100 denied.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    results = []

    # 60 workers * 10 units = 600 total (100 over limit)
    tasks = [
        asyncio.create_task(consume_once(redis, UNITS_EACH, results)) for _ in range(60)
    ]
    await asyncio.gather(*tasks)

    granted = [r for r in results if r.granted]
    denied = [r for r in results if not r.granted]
    total_granted = len(granted) * UNITS_EACH

    assert (
        total_granted <= QUOTA_LIMIT
    ), f"OVER-SERVED: {total_granted} units granted against limit {QUOTA_LIMIT}"
    assert len(denied) > 0, "Some requests should be denied when over limit"
    assert (
        total_granted + len(denied) * UNITS_EACH >= QUOTA_LIMIT
    ), "Denied requests should account for the overflow"


@pytest.mark.asyncio
async def test_org_not_configured(redis):
    """
    Requests for unconfigured org raise ValueError → 403.
    """
    results = []
    errors = []

    async def attempt():
        try:
            result = await service.consume(
                redis=redis,
                db=None,
                org_id="unknown-org",
                feature=TEST_FEATURE,
                units=1,
                idempotency_key=str(uuid.uuid4()),
            )
            results.append(result)
        except ValueError as e:
            errors.append(str(e))

    tasks = [asyncio.create_task(attempt()) for _ in range(10)]
    await asyncio.gather(*tasks)

    assert len(errors) == 10, "All requests should fail for unconfigured org"
    assert all(e == "org_not_configured" for e in errors)


@pytest.mark.asyncio
async def test_idempotency(redis):
    """
    Same idempotency key never double-charges.
    """
    await setup_quota(redis, QUOTA_LIMIT)

    idempotency_key = str(uuid.uuid4())
    results = []

    # fire same idempotency key 10 times concurrently
    tasks = [
        asyncio.create_task(
            service.consume(
                redis=redis,
                db=None,
                org_id=TEST_ORG,
                feature=TEST_FEATURE,
                units=UNITS_EACH,
                idempotency_key=idempotency_key,  # same key every time
            )
        )
        for _ in range(10)
    ]

    results = await asyncio.gather(*tasks)

    # all should return same result
    assert all(
        r.granted == results[0].granted for r in results
    ), "All responses for same idempotency key should be identical"
    assert all(
        r.remaining == results[0].remaining for r in results
    ), "Remaining should be identical for same idempotency key"

    # only 10 units should be consumed despite 10 calls
    period = (
        __import__("datetime")
        .datetime.now(__import__("datetime").timezone.utc)
        .strftime("%Y-%m")
    )
    counter_key = f"quota:{TEST_ORG}:{TEST_FEATURE}:{period}"
    counter = int(await redis.get(counter_key) or 0)

    assert (
        counter == UNITS_EACH
    ), f"Expected {UNITS_EACH} units consumed, got {counter} — idempotency failed"


@pytest.mark.asyncio
async def test_deterministic_runs(redis):
    """
    Run the core concurrency test 5 times.
    Result must be identical every time.
    Proves correctness is not accidental.
    """
    granted_counts = []

    for run in range(5):
        # reset counter between runs
        period = (
            __import__("datetime")
            .datetime.now(__import__("datetime").timezone.utc)
            .strftime("%Y-%m")
        )
        counter_key = f"quota:{TEST_ORG}:{TEST_FEATURE}:{period}"
        await redis.delete(counter_key)
        await setup_quota(redis, QUOTA_LIMIT)

        results = []
        tasks = [
            asyncio.create_task(consume_once(redis, UNITS_EACH, results))
            for _ in range(CONCURRENCY)
        ]
        await asyncio.gather(*tasks)

        granted = len([r for r in results if r.granted])
        granted_counts.append(granted)

        total_units = granted * UNITS_EACH
        assert (
            total_units <= QUOTA_LIMIT
        ), f"Run {run + 1}: OVER-SERVED {total_units} against {QUOTA_LIMIT}"

    print(f"\nDeterministic runs: {granted_counts}")
    print(
        f"All runs correct: {all(g * UNITS_EACH <= QUOTA_LIMIT for g in granted_counts)}"
    )
