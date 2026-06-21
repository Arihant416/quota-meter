# Quota Metering Engine — Design and Implementation Notes

---

## What This Is

A per-customer, per-feature monthly quota enforcement system built as a horizontally scalable FastAPI service. Each customer organization gets a configurable monthly allowance for specific API features. Every inbound request checks and deducts from that allowance atomically before the underlying feature executes.

The key question is to make four explicit decisions: concurrent correctness, batch behavior, failure and retry semantics, and reset and reporting. Everything else — storage, integration shape, failure posture — follows from those four.

---

## AI Assistance Disclosure

I used AI as a design partner and sounding board throughout this project. I want to be specific about what that means in practice:

**Where AI helped:**

- Brainstorming the initial architecture and talking through tradeoffs (cache-aside vs warm Redis, lazy loading vs permanent warmup, etc.)
- Explaining Lua syntax and Redis Lua semantics — I hadn't written this complicated Lua before this
- Generating the initial test scaffolding which I then rewrote significantly
- Suggesting FastAPI-specific patterns like `Depends()` and lifespan context managers
- Pointing out the idempotency race condition in my first implementation

**What I designed and decided myself:**

- The four weighted decisions (all-or-nothing batch, deduct+refund semantics, calendar month UTC, Lua for atomicity)
- Moving idempotency handling inside the Lua script itself (the key architectural insight)
- Tying refunds to the original consume request record rather than accepting arbitrary unit amounts
- The Redis key schema and cluster-readiness decisions
- The request record design (storing granted/units/refunded state alongside the idempotency result)
- Every tradeoff documented in this file

---

## Architecture

```
Incoming Request
      ↓
FastAPI Instance (3 running, stateless)
      ↓
quota_guard() — Depends() injection
      ↓
service.consume() — business logic
      ↓
store.atomic_consume() — Redis Lua script
      ↓
Shared Redis ← all quota state lives here
      ↑
MongoDB ← quota config source of truth, warmed into Redis at startup
```

Three FastAPI instances share one Redis and one MongoDB. Instances are completely stateless — they hold no quota state in memory. Adding or removing instances has no effect on correctness.

---

## The Four Weighted Decisions

### 1. Concurrent Correctness

**Decision: Idempotency and quota deduction handled atomically inside a single Lua script.**

The naive approach is a read-then-write sequence:

```
GET counter → check if enough → INCRBY counter
```

Between the GET and the INCRBY, another instance reads the same stale value. Both see sufficient quota. Both deduct. Quota goes negative or over-serves. This is a TOCTOU (Time Of Check, Time Of Use) race condition and it's explicitly what the assignment warns against.

My first implementation put the idempotency check in Python before calling the Lua script, which introduced a second race: two concurrent requests with the same idempotency key could both miss the cache simultaneously, both proceed to the Lua script, and both deduct — exactly defeating the purpose of idempotency.

**What I rejected:**

- `WATCH/MULTI/EXEC` (optimistic locking): under high contention for the same org, transaction aborts spike into a retry storm. Unacceptable at 10k TPS.
- Distributed lock (Redlock): adds latency and failure modes. The Lua script is already the lock.
- Per-instance counters: ruled out immediately. Instances come and go, state diverges.
- Python-layer idempotency check before Lua: race condition as described above.

**What I actually did:**

Both the idempotency lookup and the quota deduction happen inside a single Lua script. Redis executes Lua scripts atomically — no other command can interleave between lines. The full sequence is:

1. Check if a request record exists for this idempotency key → if yes, return cached result immediately
2. Load the configured limit
3. Load current usage
4. If remaining ≥ requested: deduct and write request record
5. If remaining < requested: write denied request record
6. Return result

Steps 1–6 execute as one indivisible operation. Two concurrent requests with the same idempotency key: one writes the record, the other reads it. No double-charge possible.

**Postcondition:** After any set of concurrent requests completes, the counter value equals the sum of all granted units and never exceeds the configured limit.

**Return codes from Lua:**

```
-1  → org not configured → HTTP 403
 1  → fresh execution (granted or denied)
 2  → replayed from existing request record
```

---

### 2. Batch Behavior

**Decision: All-or-nothing. Partial fulfillment rejected.**

A single request can consume many units — uploading 100 containers costs 100 units atomically. If an org has 60 units remaining and submits 100 containers, the entire request is denied. Nothing is deducted.

**Why not partial fulfillment:**

In a supply chain context, partial tracking is semantically broken. If you upload 100 containers and only 60 get tracked, which 60? The caller has no clean way to handle partial success without knowing which items succeeded. All-or-nothing gives a clean contract: full grant or full denial.

**The accepted tradeoff:** A large batch can block a nearly-exhausted quota even though smaller requests could still succeed. Callers should check their usage before submitting large batches. The usage endpoint exists for exactly this reason.

---

### 3. Failure and Retry Semantics

**This is the most nuanced decision.**

**What I chose: deduct-then-compensate with request-tied refunds.**

The consume path performs an immediate atomic deduction. If the downstream operation fails after a successful deduction, the caller issues a refund via `POST /api/v1/quota/refund` with the original consume idempotency key. The system refunds exactly the units from that original request — the caller cannot specify an arbitrary refund amount.

**Why this design:**

- Keeps the hot path to a single atomic Redis operation
- Refund is tied to the request record, not a caller-supplied unit count — prevents over-refunding
- Refunds are themselves idempotent: the request record tracks a `refunded` flag, so calling refund twice is safe

**The honest limitation:**

This is compensating transaction semantics, not reservation semantics. If a caller deducts quota and then crashes before issuing a refund, quota is permanently consumed for that period until manual reconciliation. This is a conscious tradeoff for implementation simplicity.

**What I would do in production:**

Move to a reserve → commit → release model: the consume call reserves units (marks them pending), the downstream operation commits or releases. Stale pending reservations expire automatically after a timeout. This is strictly more robust but adds significant complexity to both the quota engine and all consumers.

**Idempotency implementation:**

Every consume request requires an `X-Idempotency-Key` header. The request record stored in Redis contains:

```json
{
  "org_id": "org1",
  "feature": "container-tracking",
  "units": 100,
  "granted": true,
  "remaining": 400,
  "period": "2026-06",
  "refunded": false
}
```

This record serves as both the idempotency cache and the refund source of truth. TTL is 7 days — long enough to cover any reasonable retry or refund window, while avoiding the memory cost of keeping records for the full 35-day billing period.

**Note on replayed responses:** When a request is replayed via idempotency key, the `remaining` field in the response reflects quota at the time of the original request, not current quota. This is standard idempotency contract behavior.

---

### 4. Reset and Reporting

**Decision: Calendar month, UTC. Period baked into the Redis key. No explicit reset operation.**

**What "monthly" means:** Calendar month. New period starts at `00:00:00 UTC` on the first of each month.

**Why calendar over rolling 30 days:** Predictability. Customers want to know "my quota resets on the 1st." Rolling windows require storing first-use timestamps and computing expiry dynamically — more complexity, less intuitive for billing.

**Why UTC:** The system uses UTC to avoid timezone ambiguity across customers and servers. A single timezone eliminates edge cases entirely.

**How reset actually happens — no cron job, no scheduled task:**

The period is part of the Redis counter key:

```
quota:{org1}:container-tracking:2026-06  →  June counter
quota:{org1}:container-tracking:2026-07  →  July counter (new key, starts at 0)
```

When July arrives, requests naturally use a new key. The old key expires via TTL (35 days). No reset operation, no scheduled job, no race condition around reset time. Self-managing.

**Usage endpoint response:**

```json
{
  "org_id": "org1",
  "feature": "container-tracking",
  "period": "2026-06",
  "limit": 500,
  "used": 320,
  "remaining": 180,
  "next_reset_at": "2026-07-01T00:00:00+00:00"
}
```

`next_reset_at` is the first moment of the next period, not the last moment of the current one. This avoids off-by-one-second ambiguity.

---

## Storage Design

### Redis Key Schema

All keys use `{org_id}` as a hash tag, ensuring all keys for a given org land on the same Redis Cluster slot. This is required for Lua script atomicity in cluster mode.

```
quota_config:{org_id}:{feature}              → configured limit (integer), no TTL
quota:{org_id}:{feature}:{YYYY-MM}           → usage counter (integer), TTL 35 days
quota_request:{org_id}:{idempotency_key}     → request record (JSON), TTL 7 days
```

Config keys have no TTL — they live until explicitly changed. Counter keys expire after 35 days. Request records expire after 7 days — sufficient to cover any retry or refund window.

### MongoDB Schema

```
collection: quota_configs
{
  "org_id":   string,
  "feature":  string,
  "limit":    integer
}
index: { org_id: 1, feature: 1 }, unique: true
```

MongoDB is the source of truth for configured limits. Redis is the operational cache. Counters and request records never touch MongoDB.

---

## Cache Strategy

On startup, the warmup script loads all quota configs from MongoDB into Redis permanently (no TTL on config keys). Redis AOF persistence is enabled, so configs survive restarts.

**Why permanent warm Redis over lazy loading:**

Lazy loading (cache-aside) creates thundering herd risk at Redis restart or month rollover — thousands of orgs simultaneously miss cache and hammer MongoDB. With 5,000 orgs × 30 features = 150,000 config keys at roughly 100 bytes each, the total warm cache is ~15MB. Trivially within Redis capacity.

**On config change:** Write-through. The admin endpoint writes to both MongoDB and Redis in the same request. Redis is always consistent with MongoDB.

**On Redis restart:** Re-run the warmup script (pipeline bulk write, completes in seconds). AOF persistence means this is a rare edge case in practice.

**Concurrent warmup across instances:** All three service instances run the warmup script on startup. Writes are idempotent (`SET` on existing keys overwrites with the same value), so concurrent warmup runs are safe and produce consistent Redis state.

---

## Integration Shape

The quota engine core (`store.py`, `service.py`) has zero FastAPI dependency. It is pure async Python that any framework can call:

```python
# works with FastAPI, Django, Flask, raw asyncio — anything
result = await service.consume(redis, db, org_id, feature, units, idempotency_key)
```

FastAPI is the demonstration shell. The `quota_guard` factory wraps the engine as a `Depends()` injection:

```python
@router.post("/track-containers")
async def track_containers(request: Request, payload: TrackContainersPayload):
    units = len(payload.containers)
    await quota_guard(feature="container-tracking", units=units)(request)
    # quota already enforced — feature logic here
```

Units are determined from the request payload (batch size), so the guard is called manually rather than as a decorator argument. This is necessary because units are not known at decoration time.

In a larger production deployment, feature services could either import the quota engine as an internal package or call it over HTTP.

---

## Failure Semantics

**Redis unreachable:** Fail closed, return HTTP 503. Failing open would allow unlimited consumption with no recovery mechanism, which is worse than a temporary outage.

**MongoDB unreachable at startup:** App refuses to start. Logged clearly, exception raised immediately.

**MongoDB unreachable at runtime:** Not a problem. MongoDB is not in the hot path after warmup. Runtime MongoDB dependency is limited to admin config updates and the usage endpoint (which reads from Redis anyway).

**Denied quota on retry:** If a caller retries a request that was originally denied, the idempotency record returns the original denied result. No additional deduction. No confusion.

---

## Corner Cases Handled

**First request of a new month:**
Counter key does not exist yet. The Lua script handles this: `tonumber(redis.call('GET', KEYS[1]) or 0)` — nil becomes 0. First request of a new period works correctly without any setup.

**Exact exhaustion:**
If remaining equals requested exactly, the request is granted. Counter hits the limit precisely. Tested explicitly.

**Concurrent same-key requests:**
Two requests with the same idempotency key arriving simultaneously — only one creates the request record inside the atomic Lua script. The other reads it. One charge, always.

**Refunding a denied request:**
The refund script checks the `granted` field in the request record. If false, returns HTTP 400 with `cannot_refund_denied_request`. You cannot refund what was never charged.

**Double refund:**
The request record tracks a `refunded` boolean. Second refund call returns idempotent success without touching the counter.

**Org not configured:**
Lua script checks for the config key first. If absent, returns HTTP 403.

**Org mismatch on refund:**
The request record stores `org_id` and `feature`. The refund script validates these match the caller's supplied values. Mismatch returns HTTP 400.

**Counter TTL preserved on refund:**
When a refund writes the updated counter back to Redis, it uses `SET ... KEEPTTL` to preserve the existing 35-day expiry. Without this, a refund would silently remove the counter's TTL and cause it to persist indefinitely.

**Cross-period refund:**
Known limitation. Refunds are tied to the current period's counter key. Refunds issued after a period rollover are effectively no-ops on the counter — the current period counter starts at 0, floors to 0, and the refund has no effect. The `refunded` flag on the request record is still set, preventing a second attempt. A production fix would store the original counter key inside the request record and always refund against that key directly.

---

## What The Tests Prove

### Unit / Concurrency Tests

```
pytest tests/test_concurrency.py -v -s

test_concurrent_over_limit_exactly_caps_at_quota   PASSED
test_exact_exhaustion                               PASSED
test_org_not_configured                             PASSED
test_same_idempotency_key_only_consumes_once        PASSED
test_refund_by_original_request_idempotency_key     PASSED
test_denied_request_cannot_be_refunded              PASSED

6 passed in 2.16s
```

**`test_concurrent_over_limit_exactly_caps_at_quota`** is the critical one:

- 100 concurrent coroutines, each requesting 10 units
- Quota limit: 500
- Total attempted: 1,000 units
- Result: exactly 50 granted, exactly 50 denied, final counter = 500

This runs directly against a real Redis instance, bypassing HTTP entirely. It proves the Lua script guarantees correctness at the engine level, independent of the web layer.

**`test_same_idempotency_key_only_consumes_once`** proves the idempotency race is closed:

- 10 concurrent coroutines, all using the same idempotency key, each requesting 10 units
- Result: all 10 return identical results, counter = 10 (charged once)

**`test_refund_by_original_request_idempotency_key`** proves refund semantics:

- Consume 25 units, refund using original idempotency key, counter returns to 0
- Second refund call is idempotent — counter stays at 0

### Load Simulator — Two Scenarios

```bash
docker-compose up --build
docker logs load-simulator
```

**Scenario A — Saturation / Over-limit**

```
Quota limit          : 500
Concurrency          : 50 workers
Requests each worker : 20
Units per request    : 1
Total requests       : 1000
Total attempted units: 1000

Granted requests : 500
Denied requests  : 500
Errors           : 0
Granted units    : 500
Over-served      : 0
Duration         : 1.51s
Throughput       : 662.77 req/s
Latency avg      : 72.25 ms
Latency p50      : 65.83 ms
Latency p95      : 101.39 ms
Latency p99      : 256.33 ms
Usage endpoint   : used=500 remaining=0 period=2026-06

PASS: quota never exceeded configured limit
```

**Scenario B — Same Idempotency Key Replay**

```
Replay requests sent    : 20
Replay request units    : 10
Replay responses 200    : 20
Replay responses 429    : 0
Replay errors           : 0
Replay final used units : 10
Replay final remaining  : 90

PASS: same idempotency key consumed quota only once
```

**On the latency numbers:** On a Linux host with lower container networking overhead and a dedicated Redis instance, latency would be expected to be materially lower than the local WSL2 numbers shown here.

**NOTE**: The correctness results (granted/denied/over-served) are environment-independent.

---

## Assumptions and Considerations

**Quota config changes mid-period:** If a limit is increased mid-month, the new limit takes effect immediately. If decreased below current usage, new requests are denied but existing usage is not rolled back.

**Clock skew:** Period calculation uses `datetime.now(timezone.utc)`. All instances use the same UTC reference. Skew is not a concern because the period string (`2026-06`) is coarse-grained.

**Idempotency key scope:** Request record keys are scoped to `{org_id}:{idempotency_key}`, preventing cross-org key collisions and ensuring all keys for an org hash to the same Redis Cluster slot.

**Request record TTL:** Request records have a 7-day TTL. If a record expires before the caller retries or issues a refund, the retry is treated as a fresh request and the original quota cannot be recovered programmatically.

**Admin endpoint authentication:** The admin endpoint has no auth — any caller can modify any org's quota. Intentional for the take-home, would need auth middleware in production.

---

## Single Command Setup

```bash
git clone https://github.com/Arihant416/quota-meter.git
cd quota-meter
docker-compose up --build
```

This brings up:

- `quota-redis` — Redis 8 with AOF persistence
- `quota-mongodb` — MongoDB 7
- `quota-service-1` → <http://localhost:8001>
- `quota-service-2` → <http://localhost:8002>
- `quota-service-3` → <http://localhost:8003>
- `load-simulator` — runs both scenarios automatically

Swagger UI: <http://localhost:8001/docs>

The load simulator waits for all three quota services to pass their health checks before firing. No manual setup needed.

> **Note on warmup:** The warmup script runs automatically on each container boot before uvicorn starts. Since all three instances write the same config values from MongoDB, concurrent warmup runs are idempotent and safe. Manual re-run is only needed if Redis restarts independently while app containers remain running.

---

## Scaling to 50,000 Organizations

The current design is intended to support the assignment's 5,000-org scale target. For a much larger deployment such as 50,000 orgs, Redis Cluster is the main operational change — the key schema is already cluster-ready.

**Redis Cluster readiness:** All three keys accessed in the Lua script share `{org_id}` as a hash tag:

```
quota:{org_id}:{feature}:{period}          ← counter
quota_config:{org_id}:{feature}            ← config
quota_request:{org_id}:{idempotency_key}   ← request record
```

All three hash to the same cluster slot. No CROSSSLOT errors. No key schema migration needed for cluster deployment.

**Warmup at scale:** 50,000 orgs × 30 features = 1.5M config keys × ~100 bytes = ~150MB. Loaded via pipeline in under 30 seconds. Within Redis capacity on standard hardware.

**What does not change at 50k orgs:**

- Lua script logic and atomicity guarantees
- Calendar month UTC reset
- All-or-nothing batch policy
- Request-tied refund semantics
- Counters never touching MongoDB

---

## What Is Out of Scope / Production Hardening Needed

**Authentication and authorization:**
Admin endpoint has no auth. Feature endpoints trust `X-Org-ID` without verification. Production needs JWT validation or API key auth.

**Reservation semantics:**
Current model is deduct + compensate. Production hardening would move to reserve → commit → release with automatic expiry of stale reservations.

**Observability:**
Currently plain Python logging. Production needs structured JSON logs, Prometheus metrics (consumption rate, denial rate, latency histograms), and distributed tracing.

**Redis high availability:**
Single Redis node is a single point of failure. Production needs Redis Sentinel or Cluster.

**Cross-period refunds:**
Refunds issued after a period rollover are effectively no-ops on the counter. Production fix: store the original counter key inside the request record and use it directly for refunds regardless of current period.

**Warmup automation:**
In the Docker setup, warmup runs automatically before each instance starts. The gap is when Redis restarts independently — the app containers don't restart, so warmup doesn't re-run. Production hardening: health gate that detects empty Redis and triggers warmup automatically without requiring a full app restart.

---

## Project Structure

```bash
quota-meter/
├── app/
│   ├── core/
│   │   └── config.py           Redis + MongoDB connection factories
│   ├── quota/
│   │   ├── models.py           Pydantic models
│   │   ├── store.py            Redis Lua scripts + raw Redis operations
│   │   ├── service.py          Business logic, response building
│   │   └── dependencies.py     FastAPI quota_guard dependency
│   ├── features/
│   │   └── routes.py           Feature endpoints + quota management
│   └── main.py                 FastAPI app, lifespan, router
├── scripts/
│   └── warmup.py               Bulk config loader MongoDB → Redis
├── load_test/
│   └── simulate.py             Two-scenario load simulator
├── tests/
│   └── test_concurrency.py     Six correctness proofs
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── pytest.ini
```

---

## Running Tests

### 1) Run the concurrency test suite

The `pytest` suite is an **integration test suite**, so it requires **Redis** and **MongoDB** to be running.

Start the required infrastructure first:

```bash
docker compose up -d redis mongodb
```

Then run the tests:

```bash
pytest -v -s
```

Once the tests are done, stop those containers before running the load simulation:

```bash
docker compose stop redis mongodb
```

---

### 2) Run the multi-instance load simulation

The load simulation starts the **full Docker Compose stack**, which includes:

- Redis
- MongoDB
- 3 quota-service instances
- 1 load-simulator container

Before starting it, make sure Redis and MongoDB from the pytest setup are not already running, otherwise you may hit a port conflict on `6379` / `27017`.

Run the full load test stack:

```bash
docker-compose up --build
```

The `load-simulator` container starts automatically and runs two scenarios:

- **Scenario A:** concurrent saturation / over-limit validation
- **Scenario B:** replaying the same idempotency key to verify idempotent behavior

To inspect the load test results:

```bash
docker logs load-simulator
```

---

### Notes

- The `pytest` suite validates correctness and concurrency behavior against real Redis and MongoDB instances.
- The load simulation validates the service under concurrent multi-instance traffic and checks that quota is never over-served.
- If you only want to run the tests, you do **not** need to start the full multi-instance stack — Redis and MongoDB are enough.

**To conclude:** This was genuinely a very interesting problem — the concurrency correctness requirement forced every other decision into a specific shape. I enjoyed working through it.
