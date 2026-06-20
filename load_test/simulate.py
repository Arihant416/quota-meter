"""
Load simulator for the quota metering service.

Goals
-----
1. Exercise quota enforcement under concurrent load across multiple service instances.
2. Verify the system never over-serves beyond the configured quota.
3. Verify same-idempotency-key retries do not double-charge.
4. Produce simple latency and correctness metrics that can be cited in DESIGN.md.

Scenarios
---------
A) Saturation / over-limit test
   - Many concurrent requests with unique idempotency keys.
   - Total attempted units > configured quota.
   - Expect granted units <= quota.

B) Idempotency replay test
   - Many concurrent requests using the SAME idempotency key.
   - Expect only one logical consumption.

Usage
-----
Local:
    python -m load_test.simulate

With custom env:
    TARGETS=http://localhost:8001,http://localhost:8002,http://localhost:8003 \
    QUOTA_LIMIT=500 \
    CONCURRENCY=50 \
    REQUESTS_EACH=20 \
    python -m load_test.simulate
"""

from __future__ import annotations

import asyncio
import logging
import os
import statistics
import time
import uuid
from dataclasses import dataclass, field

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

TARGETS = [
    t.strip()
    for t in os.getenv(
        "TARGETS",
        "http://localhost:8001,http://localhost:8002,http://localhost:8003",
    ).split(",")
    if t.strip()
]

ORG_ID = os.getenv("ORG_ID", "load-test-org")
FEATURE = os.getenv("FEATURE", "container-tracking")

# Scenario A (saturation / over-limit)
QUOTA_LIMIT = int(os.getenv("QUOTA_LIMIT", "500"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "50"))
REQUESTS_EACH = int(os.getenv("REQUESTS_EACH", "20"))
UNITS_PER_REQUEST = int(os.getenv("UNITS_PER_REQUEST", "1"))

# Scenario B (same idempotency replay)
IDEMPOTENCY_REPLAY_CONCURRENCY = int(os.getenv("IDEMPOTENCY_REPLAY_CONCURRENCY", "20"))
IDEMPOTENCY_REPLAY_UNITS = int(os.getenv("IDEMPOTENCY_REPLAY_UNITS", "10"))

REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))


# ------------------------------------------------------------------------------
# Data structures
# ------------------------------------------------------------------------------


@dataclass
class RequestRecord:
    status: int
    latency_ms: float
    error: str | None = None


@dataclass
class ScenarioResult:
    name: str
    granted: int = 0
    denied: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def add(self, status: int, latency_ms: float, error: str | None = None) -> None:
        self.latencies_ms.append(latency_ms)

        if error is not None:
            self.errors += 1
            return

        if status == 200:
            self.granted += 1
        elif status == 429:
            self.denied += 1
        else:
            self.errors += 1

    @property
    def total(self) -> int:
        return self.granted + self.denied + self.errors

    @property
    def avg_ms(self) -> float:
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def p50_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return statistics.median(self.latencies_ms)

    @property
    def p95_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        arr = sorted(self.latencies_ms)
        idx = min(len(arr) - 1, int(len(arr) * 0.95))
        return arr[idx]

    @property
    def p99_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        arr = sorted(self.latencies_ms)
        idx = min(len(arr) - 1, int(len(arr) * 0.99))
        return arr[idx]


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def build_track_payload(units: int, worker_id: int, request_id: int) -> dict:
    containers = [f"CONT-{worker_id}-{request_id}-{i}" for i in range(units)]
    return {"containers": containers}


def target_for(worker_id: int) -> str:
    # deterministic round-robin target selection across instances
    return TARGETS[worker_id % len(TARGETS)]


async def wait_for_health(
    session: aiohttp.ClientSession, target: str, attempts: int = 30
) -> None:
    health_url = f"{target}/health"

    for attempt in range(1, attempts + 1):
        try:
            async with session.get(health_url) as resp:
                if resp.status == 200:
                    return
        except Exception:
            pass

        await asyncio.sleep(1)

    raise RuntimeError(f"Service at {target} did not become healthy in time")


async def configure_quota(session: aiohttp.ClientSession, limit: int) -> None:
    url = f"{TARGETS[0]}/api/v1/admin/quota/config"
    payload = {
        "org_id": ORG_ID,
        "feature": FEATURE,
        "limit": limit,
    }

    async with session.post(url, json=payload) as resp:
        body = await resp.text()
        if resp.status != 200:
            raise RuntimeError(
                f"Failed to configure quota. status={resp.status}, body={body}"
            )

    logger.info("Configured quota: org=%s feature=%s limit=%s", ORG_ID, FEATURE, limit)


async def fetch_usage(session: aiohttp.ClientSession) -> dict:
    url = f"{TARGETS[0]}/api/v1/quota/usage"
    params = {"org_id": ORG_ID, "feature": FEATURE}

    async with session.get(url, params=params) as resp:
        body = await resp.text()
        if resp.status != 200:
            raise RuntimeError(
                f"Failed to fetch usage. status={resp.status}, body={body}"
            )
        return await resp.json()


async def post_track_containers(
    session: aiohttp.ClientSession,
    target: str,
    payload: dict,
    idempotency_key: str,
) -> RequestRecord:
    url = f"{target}/api/v1/track-containers"
    headers = {
        "X-Org-ID": ORG_ID,
        "X-Idempotency-Key": idempotency_key,
    }

    start = time.perf_counter()
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            latency_ms = (time.perf_counter() - start) * 1000
            _ = await resp.text()  # drain response
            return RequestRecord(status=resp.status, latency_ms=latency_ms)
    except Exception as exc:
        latency_ms = (time.perf_counter() - start) * 1000
        return RequestRecord(status=0, latency_ms=latency_ms, error=str(exc))


# ------------------------------------------------------------------------------
# Scenario A: saturation / over-limit
# ------------------------------------------------------------------------------


async def saturation_worker(
    worker_id: int,
    session: aiohttp.ClientSession,
    result: ScenarioResult,
) -> None:
    target = target_for(worker_id)

    for req_idx in range(REQUESTS_EACH):
        idem = str(uuid.uuid4())
        payload = build_track_payload(UNITS_PER_REQUEST, worker_id, req_idx)

        record = await post_track_containers(
            session=session,
            target=target,
            payload=payload,
            idempotency_key=idem,
        )
        result.add(record.status, record.latency_ms, record.error)


async def run_saturation_scenario(session: aiohttp.ClientSession) -> ScenarioResult:
    logger.info("")
    logger.info("=" * 80)
    logger.info("SCENARIO A: saturation / over-limit")
    logger.info("=" * 80)
    logger.info("Targets              : %s", TARGETS)
    logger.info("Quota limit          : %s", QUOTA_LIMIT)
    logger.info("Concurrency          : %s", CONCURRENCY)
    logger.info("Requests each worker : %s", REQUESTS_EACH)
    logger.info("Units per request    : %s", UNITS_PER_REQUEST)
    logger.info("Total requests       : %s", CONCURRENCY * REQUESTS_EACH)
    logger.info(
        "Total attempted units: %s", CONCURRENCY * REQUESTS_EACH * UNITS_PER_REQUEST
    )

    await configure_quota(session, QUOTA_LIMIT)

    result = ScenarioResult(name="saturation")
    start = time.perf_counter()

    tasks = [
        asyncio.create_task(saturation_worker(i, session, result))
        for i in range(CONCURRENCY)
    ]
    await asyncio.gather(*tasks)

    duration_s = time.perf_counter() - start
    usage = await fetch_usage(session)

    granted_units = result.granted * UNITS_PER_REQUEST
    over_served = max(0, granted_units - QUOTA_LIMIT)

    logger.info("")
    logger.info("Scenario A results")
    logger.info("-" * 80)
    logger.info("Granted requests : %s", result.granted)
    logger.info("Denied requests  : %s", result.denied)
    logger.info("Errors           : %s", result.errors)
    logger.info("Granted units    : %s", granted_units)
    logger.info("Quota limit      : %s", QUOTA_LIMIT)
    logger.info("Over-served      : %s", over_served)
    logger.info("Duration         : %.2fs", duration_s)
    logger.info(
        "Throughput       : %.2f req/s", result.total / duration_s if duration_s else 0
    )
    logger.info("Latency avg      : %.2f ms", result.avg_ms)
    logger.info("Latency p50      : %.2f ms", result.p50_ms)
    logger.info("Latency p95      : %.2f ms", result.p95_ms)
    logger.info("Latency p99      : %.2f ms", result.p99_ms)
    logger.info(
        "Usage endpoint   : used=%s remaining=%s period=%s",
        usage["used"],
        usage["remaining"],
        usage["period"],
    )

    if over_served > 0:
        logger.error("FAIL: quota was over-served")
    elif result.errors > 0:
        logger.warning("WARN: scenario completed with request errors")
    else:
        logger.info("PASS: quota never exceeded configured limit")

    return result


# ------------------------------------------------------------------------------
# Scenario B: same-idempotency replay
# ------------------------------------------------------------------------------


async def run_idempotency_replay_scenario(session: aiohttp.ClientSession) -> None:
    logger.info("")
    logger.info("=" * 80)
    logger.info("SCENARIO B: same-idempotency-key replay")
    logger.info("=" * 80)

    # Use a fresh org/feature quota to avoid interference from scenario A
    replay_org = f"{ORG_ID}-idem"
    replay_feature = FEATURE
    replay_limit = 100

    # configure replay org quota
    url = f"{TARGETS[0]}/api/v1/admin/quota/config"
    payload = {
        "org_id": replay_org,
        "feature": replay_feature,
        "limit": replay_limit,
    }
    async with session.post(url, json=payload) as resp:
        body = await resp.text()
        if resp.status != 200:
            raise RuntimeError(
                f"Failed to configure replay quota. status={resp.status}, body={body}"
            )

    same_idem = str(uuid.uuid4())
    replay_target = TARGETS[0]
    replay_url = f"{replay_target}/api/v1/track-containers"
    replay_payload = {
        "containers": [f"IDEM-CONT-{i}" for i in range(IDEMPOTENCY_REPLAY_UNITS)]
    }

    async def single_replay_attempt() -> RequestRecord:
        headers = {
            "X-Org-ID": replay_org,
            "X-Idempotency-Key": same_idem,
        }

        start = time.perf_counter()
        try:
            async with session.post(
                replay_url, json=replay_payload, headers=headers
            ) as resp:
                latency_ms = (time.perf_counter() - start) * 1000
                _ = await resp.text()
                return RequestRecord(status=resp.status, latency_ms=latency_ms)
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return RequestRecord(status=0, latency_ms=latency_ms, error=str(exc))

    tasks = [
        asyncio.create_task(single_replay_attempt())
        for _ in range(IDEMPOTENCY_REPLAY_CONCURRENCY)
    ]
    records = await asyncio.gather(*tasks)

    granted = sum(1 for r in records if r.status == 200 and r.error is None)
    denied = sum(1 for r in records if r.status == 429 and r.error is None)
    errors = sum(
        1 for r in records if r.error is not None or (r.status not in (200, 429))
    )

    # verify actual usage for replay org
    usage_url = f"{TARGETS[0]}/api/v1/quota/usage"
    async with session.get(
        usage_url,
        params={"org_id": replay_org, "feature": replay_feature},
    ) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(
                f"Failed to fetch replay usage. status={resp.status}, body={body}"
            )
        usage = await resp.json()

    logger.info("Replay requests sent        : %s", IDEMPOTENCY_REPLAY_CONCURRENCY)
    logger.info("Replay request units        : %s", IDEMPOTENCY_REPLAY_UNITS)
    logger.info("Replay responses 200        : %s", granted)
    logger.info("Replay responses 429        : %s", denied)
    logger.info("Replay errors               : %s", errors)
    logger.info("Replay final used units     : %s", usage["used"])
    logger.info("Replay final remaining      : %s", usage["remaining"])

    if usage["used"] != IDEMPOTENCY_REPLAY_UNITS:
        logger.error(
            "FAIL: same idempotency key should have consumed %s units once, but usage shows %s",
            IDEMPOTENCY_REPLAY_UNITS,
            usage["used"],
        )
    else:
        logger.info("PASS: same idempotency key consumed quota only once")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


async def main() -> None:
    logger.info("Starting load simulator")
    logger.info("Targets: %s", TARGETS)

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # wait for all targets to be healthy
        for target in TARGETS:
            await wait_for_health(session, target)

        await run_saturation_scenario(session)
        await run_idempotency_replay_scenario(session)

    logger.info("")
    logger.info("Load simulation complete")


if __name__ == "__main__":
    asyncio.run(main())
