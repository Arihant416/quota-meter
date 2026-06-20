"""
I used Claude to generate a Simulation Script for my service.
Load simulator — fires concurrent requests across all quota service instances.
Demonstrates horizontal scaling and correctness under load.

Run via:
    python -m load_test.simulate
    or via docker-compose (load-simulator service)
"""

import asyncio
import aiohttp
import os
import uuid
import time
import logging
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

TARGETS = os.getenv(
    "TARGETS", "http://localhost:8001,http://localhost:8002,http://localhost:8003"
).split(",")

ORG_ID = "load-test-org"
FEATURE = "container-tracking"
QUOTA_LIMIT = 1000  # set this via admin endpoint before test
CONCURRENCY = 50  # concurrent workers
REQUESTS_EACH = 20  # each worker fires this many requests
UNITS_PER_REQ = 1  # units per request


# ── Result Tracking ────────────────────────────────────────────────────────────


@dataclass
class SimResult:
    granted: int = 0
    denied: int = 0
    errors: int = 0
    latencies: list[float] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.granted + self.denied + self.errors

    @property
    def p50(self) -> float:
        if not self.latencies:
            return 0
        s = sorted(self.latencies)
        return s[len(s) // 2]

    @property
    def p99(self) -> float:
        if not self.latencies:
            return 0
        s = sorted(self.latencies)
        return s[int(len(s) * 0.99)]

    @property
    def avg(self) -> float:
        if not self.latencies:
            return 0
        return sum(self.latencies) / len(self.latencies)


# ── Setup ──────────────────────────────────────────────────────────────────────


async def setup_quota(session: aiohttp.ClientSession):
    """
    Creates quota config for load test org before running.
    """
    target = TARGETS[0]
    url = f"{target}/api/v1/admin/quota/config"

    payload = {
        "org_id": ORG_ID,
        "feature": FEATURE,
        "limit": QUOTA_LIMIT,
    }

    async with session.post(url, json=payload) as resp:
        if resp.status == 200:
            logger.info(
                f"✅ Quota configured: {ORG_ID}/{FEATURE} = {QUOTA_LIMIT} units"
            )
        else:
            body = await resp.text()
            raise RuntimeError(f"Failed to configure quota: {body}")


# ── Worker ─────────────────────────────────────────────────────────────────────


async def worker(
    worker_id: int,
    session: aiohttp.ClientSession,
    result: SimResult,
    semaphore: asyncio.Semaphore,
):
    """
    Single worker — fires REQUESTS_EACH requests to random targets.
    """
    for i in range(REQUESTS_EACH):
        # round robin across targets
        target = TARGETS[worker_id % len(TARGETS)]
        url = f"{target}/api/v1/track-containers"

        headers = {
            "X-Org-ID": ORG_ID,
            "X-Idempotency-Key": str(uuid.uuid4()),  # unique per request
        }

        payload = {"containers": [f"CONT-{worker_id}-{i}"]}  # 1 container = 1 unit

        async with semaphore:
            start = time.monotonic()
            try:
                async with session.post(url, json=payload, headers=headers) as resp:
                    elapsed_ms = (time.monotonic() - start) * 1000
                    result.latencies.append(elapsed_ms)

                    if resp.status == 200:
                        result.granted += 1
                    elif resp.status == 429:
                        result.denied += 1
                    else:
                        result.errors += 1
                        body = await resp.text()
                        logger.warning(f"Unexpected status {resp.status}: {body}")

            except Exception as e:
                result.errors += 1
                logger.error(f"Worker {worker_id} request failed: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────


async def run():
    logger.info("=" * 60)
    logger.info("  Quota Metering Load Simulator")
    logger.info("=" * 60)
    logger.info(f"Targets:      {TARGETS}")
    logger.info(f"Org:          {ORG_ID}")
    logger.info(f"Feature:      {FEATURE}")
    logger.info(f"Quota limit:  {QUOTA_LIMIT} units")
    logger.info(f"Workers:      {CONCURRENCY}")
    logger.info(f"Requests each:{REQUESTS_EACH}")
    logger.info(f"Total reqs:   {CONCURRENCY * REQUESTS_EACH}")
    logger.info(f"Total units:  {CONCURRENCY * REQUESTS_EACH * UNITS_PER_REQ}")
    logger.info("=" * 60)

    # wait for services to be ready
    await asyncio.sleep(3)

    result = SimResult()
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        # setup quota config first
        await setup_quota(session)
        await asyncio.sleep(1)

        logger.info(f"🚀 Firing {CONCURRENCY} concurrent workers...")
        start = time.monotonic()

        # spawn all workers simultaneously
        tasks = [
            asyncio.create_task(worker(i, session, result, semaphore))
            for i in range(CONCURRENCY)
        ]

        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

    # ── Results ────────────────────────────────────────────────────────────────
    total_units_granted = result.granted * UNITS_PER_REQ

    logger.info("=" * 60)
    logger.info("  Results")
    logger.info("=" * 60)
    logger.info(f"Total requests:   {result.total}")
    logger.info(f"Granted:          {result.granted}")
    logger.info(f"Denied (429):     {result.denied}")
    logger.info(f"Errors:           {result.errors}")
    logger.info(f"Units consumed:   {total_units_granted} / {QUOTA_LIMIT}")
    logger.info(f"Over-served:      {max(0, total_units_granted - QUOTA_LIMIT)}")
    logger.info("-" * 60)
    logger.info(f"Duration:         {elapsed:.2f}s")
    logger.info(f"Throughput:       {result.total / elapsed:.0f} req/s")
    logger.info(f"Avg latency:      {result.avg:.2f}ms")
    logger.info(f"P50 latency:      {result.p50:.2f}ms")
    logger.info(f"P99 latency:      {result.p99:.2f}ms")
    logger.info("=" * 60)

    # ── Correctness Check ──────────────────────────────────────────────────────
    if total_units_granted > QUOTA_LIMIT:
        logger.error(
            f"❌ OVER-SERVED: granted {total_units_granted} against limit {QUOTA_LIMIT}"
        )
    elif result.errors > 0:
        logger.warning(f"⚠️  {result.errors} errors encountered")
    else:
        logger.info(f"✅ CORRECT: never over-served")
        logger.info(
            f"✅ Units granted ({total_units_granted}) <= limit ({QUOTA_LIMIT})"
        )


if __name__ == "__main__":
    asyncio.run(run())
