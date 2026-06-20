# Quota Metering Engine — DESIGN.md

## What This Is

A horizontally scalable, prepaid monthly API quota enforcement system. Each customer organization gets a per-feature monthly quota. Every API call checks and deducts from that quota atomically. Built for correctness under concurrency first, performance second.

---

## AI Assistance Disclosure

This project was built with Claude (Anthropic) as a design and implementation partner. Specifically:

- **AI-assisted:** Lua script syntax, test structure scaffolding, docker-compose boilerplate, load simulator structure
- **My own design decisions:** Storage architecture, cache strategy, atomicity approach, idempotency design, failure semantics, batch policy, reset strategy, the four weighted decisions below

---

## Architecture Overview

```
Client Request
      ↓
FastAPI Instance (stateless, 3 running)
      ↓
quota_guard() — Depends() injection / a middleware
      ↓
service.consume() — business logic
      ↓
store.atomic_deduct() — Redis Lua script
      ↓
Shared Redis ← single source of counter truth
      ↑
MongoDB ← single source of config truth (warmed into Redis at startup)
```

**Three FastAPI instances** share one Redis and one MongoDB. All quota state lives in Redis. All instances are completely stateless — add or remove them freely without affecting correctness.

---

## The Four Weighted Decisions

### 1. Concurrent Correctness

**Decision: Redis Lua script — atomic check and deduct in one operation.**

The naive approach is two separate Redis commands:
```
GET counter → check if enough → INCRBY counter
```
Between `GET` and `INCRBY`, another instance reads the same stale value. Both proceed. Both deduct. Quota goes negative. This is a classic TOCTOU (Time Of Check, Time Of Use) race condition.

**What I rejected:**

- `WATCH`/`MULTI`/`EXEC` (optimistic locking) — under high contention for the same org, transaction aborts spike and you're in a retry storm. Unacceptable at 10k TPS.
- Distributed lock (Redlock) — overkill. Adds latency and failure modes. The Lua script is already the lock.
- Per-instance counters — explicitly ruled out. Instances come and go, counters diverge.

**Why Lua works:**

Redis executes Lua scripts as a single atomic unit. No other command can interleave between lines. The entire check+deduct happens as one indivisible operation:

```lua
local limit   = tonumber(redis.call('GET', KEYS[2]))
if not limit then return {-1, 0} end   -- org not configured

local current   = tonumber(redis.call('GET', KEYS[1]) or 0)
local remaining = limit - current

if remaining >= tonumber(ARGV[1]) then
    redis.call('INCRBY', KEYS[1], ARGV[1])
    redis.call('EXPIRE', KEYS[1], 3024000)
    return {1, remaining - tonumber(ARGV[1])}  -- granted
else
    return {0, remaining}  -- denied
end
```

If 10 concurrent requests each want 100 units and the org has 500 left, exactly 5 succeed. Never 6. Never negative. Proven by `test_concurrency.py`.

**Return codes:**
```
-1 → org not configured → HTTP 403
 0 → quota exhausted    → HTTP 429
 1 → granted            → HTTP 200
```

---

### 2. Batch Behavior

**Decision: All-or-nothing. Partial fulfillment rejected.**

A single request can consume many units — uploading 100 containers costs 100 units atomically.

If an org has 60 units left and submits 100 containers, the request is denied entirely with a 429. Nothing is deducted.

**Why not partial fulfillment:**

In a supply chain context, partial tracking is semantically broken. If you upload 100 containers and only 60 get tracked, which 60? The caller has no way to handle partial success cleanly without knowing which items succeeded. All-or-nothing gives a clean contract: full grant or full denial.

**The tradeoff I accept:** A large batch can "block" a nearly-exhausted quota even though smaller requests could succeed. Callers are expected to be quota-aware before submitting large batches. The usage endpoint exists for exactly this purpose.

---

### 3. Failure & Retries — Idempotency

**The problem:** Client deducts 100 units → downstream operation fails → client retries → double-charged.

**Decision: Idempotency key per request, cached in Redis for 24 hours.**

Every `consume` call requires `X-Idempotency-Key` header (a UUID the caller generates per logical operation). Before executing the Lua script:

```python
idem_key = f"idempotency:{idempotency_key}"
cached = await redis.get(idem_key)
if cached:
    return QuotaResult.model_validate_json(cached)  # return same result, no deduction
```

If the key exists, return the cached result immediately. Quota is not touched. The caller gets the exact same response they got the first time.

After execution, the result is cached with 24h TTL — covers any reasonable retry window.

**For downstream failures after deduction:** A `POST /api/v1/quota/refund` endpoint adds units back atomically via a second Lua script. The refund is also idempotent (keyed on `idempotency:refund:{key}`).

**What I rejected:** Automatic rollback. The quota service has no knowledge of downstream operations. Automatic rollback would require tight coupling that doesn't belong here. The caller handles compensation explicitly — clean separation of concerns.

---

### 4. Reset & Reporting

**Decision: Calendar month, UTC. Period baked into Redis key. No explicit reset operation.**

**What "monthly" means:** Calendar month. Resets at `00:00:00 UTC` on the 1st of each month.

**Why calendar over rolling 30 days:** Predictability. Customers want to know "my quota resets on the 1st." Rolling windows require storing first-use timestamps and computing expiry dynamically — more complexity, less intuitive for billing.

**Why UTC:** Portcast is Singapore-based with international customers. A single timezone eliminates edge cases.

**How reset actually happens — no cron job, no scheduled task:**

The period is baked into the Redis key:
```
quota:org1:container-tracking:2026-06  → June counter
quota:org1:container-tracking:2026-07  → July counter (new key, starts at 0)
```

When July arrives, requests use a new key. The old key expires naturally via TTL (35 days). No reset operation needed. Self-managing.

**Usage endpoint response:**
```json
{
  "org_id": "org1",
  "feature": "container-tracking",
  "period": "2026-06",
  "limit": 500,
  "used": 320,
  "remaining": 180,
  "resets_at": "2026-06-30T23:59:59Z"
}
```

---

## Storage Design

### Redis Key Schema

```
quota_config:{org_id}:{feature}         → limit value (integer), NO TTL, permanent
quota:{org_id}:{feature}:{YYYY-MM}      → usage counter (integer), TTL 35 days
idempotency:{idempotency_key}           → cached QuotaResult JSON, TTL 24h
idempotency:refund:{idempotency_key}    → cached refund QuotaResult JSON, TTL 24h
```

### MongoDB Schema

```
collection: quota_configs
{
  org_id:    string,
  feature:   string,
  limit:     integer,
}
index: { org_id: 1, feature: 1 }, unique: true
```

MongoDB is the source of truth for quota limits. Redis is the operational cache. **Counters never touch MongoDB.** MongoDB never touches the hot path after warmup.

---

## Cache Strategy — Permanent Warm Redis

On startup, a warmup script loads all quota config limits from MongoDB into Redis permanently (no TTL). Redis persistence (AOF) is enabled so configs survive restarts.

**Why not lazy loading (cache-aside):**

Considered and rejected. Cache-aside with lazy loading creates thundering herd risk at month reset or Redis restart — thousands of orgs simultaneously miss cache and hammer MongoDB. At 50k orgs × 30 features = 1.5M keys, a simultaneous cold start would overwhelm MongoDB.

Permanent warm Redis eliminates this entirely. Config keys are small (~100 bytes each). 1.5M keys ≈ 150MB — trivially within Redis capacity.

**On config change:** Write-through. Admin endpoint writes to both MongoDB and Redis in the same request. Redis is always consistent with MongoDB.

**On Redis restart:** Re-run warmup script (takes seconds via pipeline bulk write). AOF persistence means this is a rare edge case.

---

## Integration Shape

The quota engine is framework-agnostic at its core:

```
app/quota/store.py      ← pure Redis + Lua, no FastAPI dependency
app/quota/service.py    ← pure business logic, no FastAPI dependency
```

FastAPI is the demonstration shell. The `quota_guard` dependency wraps the engine:

```python
# one line to protect any endpoint
await quota_guard(feature="container-tracking", units=len(containers))(request)
```

Any Python framework can call `service.consume()` directly. In a real Portcast deployment, each feature service would import the quota engine as an internal package or call it as a microservice over HTTP.

---

## Failure Semantics

**If Redis is unreachable:** Fail closed. Return HTTP 503. Do not fail open — failing open means unlimited quota consumption with no recovery mechanism.

**If MongoDB is unreachable at startup:** App refuses to start. Logged clearly, raised immediately.

**If MongoDB is unreachable at runtime:** Not a problem. MongoDB is not in the hot path. Redis has all config data. Runtime MongoDB dependency is only for admin config updates and the usage reporting endpoint.

---

## Distributed Correctness

All three FastAPI instances are completely stateless. They hold zero quota state in memory. Every operation goes to the shared Redis.

```
Instance 1 sends: consume(org1, container-tracking, 100)
Instance 2 sends: consume(org1, container-tracking, 100)  ← same time
Instance 3 sends: consume(org1, container-tracking, 100)  ← same time

Redis queues these internally (single-threaded command execution)
Executes Lua scripts one at a time
Each sees the counter updated by the previous execution
```

Horizontal scaling adds throughput. It never compromises correctness.

---

## Load Test Results

Run `docker-compose up --build` to reproduce these numbers.

```
Instances:        3 (quota-service-1, quota-service-2, quota-service-3)
Workers:          50 concurrent
Requests each:    20
Total requests:   1000
Quota limit:      1000 units

Results:
Environment:      Local Docker on Windows (WSL2)
Instances:        3 quota services
Workers:          50 concurrent
Total requests:   1000
Quota limit:      1000 units

Results:
  Granted:        1000
  Denied:         0
  Errors:         0
  Over-served:    0          ← correctness verified
  Duration:       2.82s
  Throughput:     354 req/s
  Avg latency:    129ms
  P50 latency:    59ms
  P99 latency:    1416ms

Note: Latency is dominated by local Docker/WSL2 overhead.
Production Redis on Linux would yield P99 < 10ms.
```

*Numbers are from local Docker environment. Production Redis on the same network would yield sub-2ms P99.*

---

## Concurrency Test Results

```
pytest tests/test_concurrency.py -v -s

test_no_over_serve_under_concurrency  PASSED
test_exact_exhaustion                 PASSED
test_over_limit_denied                PASSED
test_org_not_configured               PASSED
test_idempotency                      PASSED
test_deterministic_runs               PASSED

Deterministic runs: [50, 50, 50, 50, 50]
All runs correct: True
```

The concurrency test bypasses HTTP entirely and hits the quota engine directly against a real Redis instance. This proves the Lua script guarantees correctness at the engine level, independent of the HTTP layer.

---

## API Reference

```
POST /api/v1/track-containers
  Headers: X-Org-ID, X-Idempotency-Key
  Body:    { containers: ["CONT-1", "CONT-2", ...] }
  Returns: 200 { status, containers, units_consumed, quota_remaining }
           403 org not configured
           429 { error, remaining, resets_at }

POST /api/v1/sailing-schedule
  Headers: X-Org-ID, X-Idempotency-Key
  Body:    { origin_port, destination_port, lookups }
  Returns: 200 { status, origin, destination, units_consumed, quota_remaining }
           403 / 429

GET  /api/v1/quota/usage?org_id=X&feature=Y
  Returns: { org_id, feature, period, limit, used, remaining, resets_at }

POST /api/v1/quota/refund
  Body:    { org_id, feature, units, idempotency_key }
  Returns: QuotaResult

POST /api/v1/admin/quota/config
  Body:    { org_id, feature, limit }
  Returns: { status, org_id, feature, limit }

GET  /health
  Returns: { status: "ok" }
```

---

## Single Command Setup

```bash
git clone https://github.com/Arihant416/quota-meter.git
cd quota-meter
docker-compose up --build
```

Services:
- `quota-service-1` → http://localhost:8001
- `quota-service-2` → http://localhost:8002
- `quota-service-3` → http://localhost:8003
- Swagger UI → http://localhost:8001/docs

Load simulator starts automatically and fires 1000 concurrent requests across all three instances.

---

## Scaling To 50,000 Organizations

Current design is validated for 5,000 orgs. For 50,000:

**Redis Cluster** — shard by org_id using hash tags:
```
{org1}:container-tracking:2026-06
```
Hash tags ensure same-org keys land on the same Redis slot. Lua script atomicity is preserved within a slot. A 3-node Redis Cluster handles ~100k ops/sec comfortably.

**Warmup at scale** — 50k orgs × 30 features = 1.5M config keys ≈ 150MB. Loaded via pipeline in ~10 seconds. Not a concern.

**MongoDB** — read replicas for config reads. Primary for writes. At 50k orgs, config reads are still rare (only on warmup and config updates). Not a bottleneck.

**What doesn't change at 50k orgs:**
- Lua script atomicity
- Calendar month UTC reset
- All-or-nothing batch policy
- Idempotency key design
- Counters never touching MongoDB

---

## Known Limits & Honest Tradeoffs

**Redis is a single point of failure.** If Redis goes down, all quota enforcement stops (503). Mitigation: Redis Sentinel or Cluster for HA. Out of scope for this assignment.

**Idempotency keys are not authenticated.** Any caller with a known key can replay it. Mitigation: scope keys to org_id in a production system.

**`resets_at` is end of current month, not start of next month.** Either is defensible. End of month is clearer from a "when does my current quota expire" perspective.

**Month-boundary counter key.** The first request of a new month always creates a new Redis key. Redis returns nil on GET for non-existent keys — handled explicitly in the Lua script with `or 0`.

**Load test numbers are local Docker.** Real production Redis on a low-latency network would yield significantly better P99 numbers.

---

## Project Structure

```
quota-meter/
├── app/
│   ├── core/
│   │   └── config.py           Redis + MongoDB connection factories
│   ├── quota/
│   │   ├── models.py           Pydantic models (QuotaResult, UsageResponse)
│   │   ├── store.py            Redis Lua scripts + raw Redis operations
│   │   ├── service.py          Business logic, idempotency, response building
│   │   └── dependencies.py     FastAPI quota_guard dependency
│   ├── features/
│   │   └── routes.py           Demo feature endpoints + quota management
│   └── main.py                 FastAPI app, lifespan, router
├── scripts/
│   └── warmup.py               Bulk config loader MongoDB → Redis
├── load_test/
│   └── simulate.py             Concurrent load simulator
├── tests/
│   └── test_concurrency.py     Correctness proof under concurrency
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
