# Quota Meter

> This is a personal engineering project focused on quota correctness under concurrency.
> The current version is intentionally small and optimized for clarity of design, correctness validation, and local reproducibility.

Quota Meter is a horizontally scalable balance/credit enforcement service for metered API features.

It is built for systems where:

- multiple stateless application instances serve traffic concurrently,
- each organization has a monthly quota per feature,
- requests can consume one or many quota units,
- retries must not double-charge customers,
- and quota enforcement must remain correct under concurrent contention.

This repository contains:

- a reusable quota engine built around **Redis + Lua**
- a FastAPI application that protects sample feature endpoints
- a Docker Compose setup with **3 quota-service instances**
- a load simulator that exercises the system under concurrent load
- concurrency tests for quota correctness, idempotency, and refund behavior

---

## What problem this service solves

Different features consume quota at different rates.

Examples:

- **container-tracking** → 1 quota unit per container tracked
- **sailing-schedule** → 1 quota unit per schedule lookup

Quotas are enforced **per organization, per feature, per month**.

The hard part is correctness under concurrency:

- many requests for the same org can arrive at the same time,
- large batch requests may consume many units at once,
- multiple app instances may race on the same shared quota,
- retries must not deduct quota more than once.

Quota Meter is designed to solve exactly that.

---

## High-Level Architecture

```text
                     +----------------------+
                     |   Client / Caller    |
                     +----------+-----------+
                                |
                                v
                  +-----------------------------+
                  |   quota-service-1 (FastAPI) |
                  +-----------------------------+
                                |
                  +-----------------------------+
                  |   quota-service-2 (FastAPI) |
                  +-----------------------------+
                                |
                  +-----------------------------+
                  |   quota-service-3 (FastAPI) |
                  +-----------------------------+
                                |
                                v
                     +----------------------+
                     |        Redis         |
                     | atomic counters      |
                     | quota config cache   |
                     | idempotency records  |
                     +----------+-----------+
                                |
                                v
                     +----------------------+
                     |       MongoDB        |
                     | quota config source  |
                     +----------------------+
```

## Core Design

### 1. Stateless application instances

The FastAPI application instances do not keep quota state in memory.

All shared quota state lives outside the app:

- Redis stores current usage counters, cached quota config, idempotency records, and refund markers
- MongoDB stores quota configuration as the durable source of truth.

This allows the service to scale horizontally without introducing per-instance quota drift.
  
### 2. Atomic quota deduction with Redis Lua

The most important correctness requirement is:
> if many requests hit a nearly exhausted quota at the same time, granted usage must never exceed the configured limit.
A naive read-then-write flow is not safe:

 1. read current usage
 2. compare with limit
 3. write updated usage
Two requests can observe the same remaining quota and both succeed.

#### Chosen approach

Quota check + deduction is performed by **a single Redis Lua script**.
The script atomically:

- reads the configured limit
- reads the current monthly usage
- checks whether enough units remain
- increments usage if allowed
- returns the result

Because Redis executes the Lua script atomically, no other request can interleave between the check and the deduction.
     - By introducing LUA we avoid
       - over-complication caused by distributed locks/mutex
       - lua acts as the lock itself

### 3. All-or-nothing batch behavior

Requests may consume multiple units in one go.
Example:

- track 100 containers → consume 100 quota units
  
#### Current behavior (Personal Design Decision)

If a request needs N units and fewer than N remain:

- the request is denied with HTTP 429
- no quota is deducted

#### Why though ?

Partial fulfillment complicates both caller behavior and downstream semantics:

- Answering which subset of a batch was accepted becomes difficult.
- how should the caller reconcile partial success?
- how does downstream business logic represent “some of the request counted”?

> `For quota metering, all-or-nothing is the cleaner contract (Again, this is a design decision based on the industry type for e.g Supply Chain would ideally go for all or nothing)`.

### 4. Idempotent consume requests

Clients may retry because of:

- timeouts
- network failures
- uncertain downstream responses

Retries should not double-charge quota.

#### Current behavior

Each consume request carries an `idempotency_key`.

- The first successful processing of that logical request is stored in Redis.
- If the same request is replayed with the same idempotency key: the cached result is returned quota is not deducted again
  
### 5. Refund by original consume request

Sometimes quota is successfully consumed, but the downstream business operation fails afterward.
Example:

- quota is granted for tracking 10 containers
- downstream tracking processing fails after quota deduction
- the caller needs to compensate the quota deduction

#### Current behavior

Refund is modeled as compensation for a previously granted consume request.
The refund request identifies the original consume request using its idempotency key:

```json
{
  "org_id": "acme-logistics",
  "feature": "container-tracking",
  "original_idempotency_key": "req-123"
}
```

The service refunds exactly the units consumed by that original request.
  
#### Why this design is safer than “`refund arbitrary units`” ?

A refund API that accepts arbitrary units is risky because the service cannot verify whether the requested refund amount corresponds to a real previously granted request.
Refunding by original consume idempotency key is safer because:

- refunds are tied to a real prior consume operation
- denied requests cannot be refunded
- the same request can be protected from being refunded twice
- the refund amount comes from persisted request state, not caller input

## Storage Model

### Redis keys

```txt
quota_config:{org_id}:{feature}
  -> cached monthly quota limit for that org + feature

quota:{org_id}:{feature}:{YYYY-MM}
  -> usage counter for the current billing month

idempotency:{idempotency_key}
  -> cached result of a consume request, used to make retries safe

refund:{original_idempotency_key}
  -> refund marker used to prevent the same consume request from being refunded twice
```

### MongoDB collection

MongoDB stores quota configuration as the durable source of truth.

Example document:

```json
{
  "org_id": "acme-logistics",
  "feature": "container-tracking",
  "limit": 500
}
```

Redis is the hot-path store for usage and request-level operational state.

MongoDB is used for configuration durability, not per-request quota mutation.

## API Overview

The FastAPI app is a thin demo integration layer around the quota engine.

### Feature endpoints

- `POST /api/v1/track-containers`
- `POST /api/v1/sailing-schedule`

These are sample quota-consuming endpoints.

### Quota endpoints

- `GET /api/v1/quota/usage`
- `POST /api/v1/quota/refund`

### Admin endpoint

- `POST /api/v1/admin/quota/config`

### Health endpoint

- `GET /health`

## Request / Response Models

### Consume request

```json
{
  "org_id": "acme-logistics",
  "feature": "container-tracking",
  "units": 10,
  "idempotency_key": "req-123"
}
```

### Semantics

- `units` must be greater than 0
- `idempotency_key` identifies the logical consume request
- replaying the same request with the same idempotency key does not consume quota twice

### Consume response

```json
{
  "granted": true,
  "remaining": 490,
  "resets_at": "2026-07-01T00:00:00Z",
  "org_id": "acme-logistics",
  "feature": "container-tracking"
}
```

### Semantics

- granted=true means quota was deducted successfully
- granted=false means the request was denied because quota was unavailable
- remaining is the remaining quota for the current monthly period after the operation

### Usage response

```json
{
  "org_id": "acme-logistics",
  "feature": "container-tracking",
  "period": "2026-06",
  "limit": 500,
  "used": 120,
  "remaining": 380,
  "resets_at": "2026-07-01T00:00:00Z"
}
```

### Refund request

```json
{
  "org_id": "acme-logistics",
  "feature": "container-tracking",
  "original_idempotency_key": "req-123"
}
```

### Refund semantics

Refund compensates a **previously granted** consume request.

The refund call does not take arbitrary units.
Instead, it identifies the original successful consume request by idempotency key and refunds exactly the units consumed by that request.

## Running the System

### Single command

```bash
docker compose up --build
```

This starts:

- Redis
- MongoDB
- quota-service-1
- quota-service-2
- quota-service-3
- load-simulator

The load simulator automatically runs after the application instances are up.

### Local service URLs

- `quota-service-1` → `http://localhost:8001`
- `quota-service-2` → `http://localhost:8002`
- `quota-service-3` → `http://localhost:8003`
- `Swagger UI` → `http://localhost:8001/docs`

## Running Tests

### Concurrency tests

```bash
pytest tests/test_concurrency.py -v -s
```

The concurrency suite validates:

- exact capping at quota under concurrent contention
- exact exhaustion behavior
- org-not-configured behavior
- same idempotency key only consuming once
- refund by original request idempotency key
- denied requests not being refundable

### Load Test

The repository includes a load simulator under load_test/simulate.py.

It currently exercises two correctness-focused scenarios.

#### Scenario A — saturation / over-limit

Many concurrent unique requests are sent across the 3 app instances with total attempted units greater than the configured quota.

#### Goal

- prove that granted usage caps exactly at the configured limit
- prove that quota is not overserved under concurrent contention

#### Scenario B — same-idempotency-key replay

Many concurrent requests reuse the same idempotency key.

#### Goal

- prove that only one logical quota consumption occurs for that request

## Real Load Test Results

The numbers below are from the current Docker run of this repository using:

- 3 FastAPI instances
- shared Redis
- shared MongoDB
- load simulation distributed across all 3 app instances

### Scenario A — saturation / over-limit

#### Test setup

- Quota limit: 500
- Concurrency: 50 workers
- Requests per worker: 20
- Units per request: 1
- Total requests: 1000
- Total attempted units: 1000

#### Observed results

- Granted requests: 500
- Denied requests: 500
- Errors: 0
- Granted units: 500
- Over-served: 0
- Final usage: used=500, remaining=0

#### Latency / throughput

- Duration: 1.82s
- Throughput: 550.74 req/s
- Avg latency: 83.91 ms
- P50: 75.96 ms
- P95: 157.72 ms
- P99: 271.39 ms

### Scenario B — same idempotency key replay

#### Observed results

- Replay requests sent: 20
- Replay request units: 10
- Replay 200 responses: 20
- Replay errors: 0
- Final used units: 10
- Final remaining: 90

### What these results prove?

- The purpose of these load tests is primarily to validate correctness under concurrent contention, not to make production latency claims from a local Docker environment.

The important outcomes are:

- quota was never overserved under concurrent saturation
- granted usage capped exactly at the configured limit
- replaying the same idempotency key only consumed quota once

The latency numbers are useful as local sanity metrics, but they should not be treated as production benchmarks.

### Docker Compose Topology

The repository runs a simple horizontally scaled local topology:

- Redis → shared hot-path state
- MongoDB → quota config source of truth
- quota-service-1
- quota-service-2
- quota-service-3
- load-simulator

The 3 application instances all point to the same Redis and MongoDB instances.

This is deliberate: it demonstrates that quota correctness does not rely on single-instance in-memory state.

## Failure Semantics

### Redis is unavailable

- Decision Taken By me : Fail closed.
- The service should not silently allow unlimited consumption if quota state is unavailable.
- (Potential Downside: Will the clients be happy if their services do not work for something they do not care about?)

### MongoDB unavailable at startup

Startup should fail because quota configuration cannot be loaded reliably.

### MongoDB unavailable during runtime

- Quota enforcement can continue as long as Redis already has the required config cached and the usage counters remain available.
- Admin quota updates may fail, but the hot path can continue to function.

## Scaling Notes

This repository is a correctness-focused service implementation, not a fully productionized multi-region quota platform.
If this were scaled significantly further, the next steps would likely be:

1. **Redis HA / clustering**

Redis is the critical shared state store. For higher availability and larger scale:

- Redis Sentinel or Redis Cluster would be the next step
- related keys should be designed carefully if cluster slot affinity is required

1. **Better observability**

Production hardening would add:

- granted / denied counters
- quota hot-path latency metrics
- Redis command timing
- refund rate
- idempotency hit rate
- per-org / per-feature usage anomalies

1. **Quota admin workflows**

The current admin config endpoint is intentionally simple.
A more complete quota platform would usually add:

- versioned config changes
- audit logging
- bulk imports / exports
- self-serve reporting

1. More explicit warmup / cache refresh controls

At larger scale, config warmup and cache refresh behavior should be observable and resilient:

- metrics around config load failures
- reconciliation tooling
- explicit behavior for partial warmup failures

## Known Tradeoffs / Limits

### Redis is a critical dependency

If Redis is down, quota enforcement cannot proceed. That is the correct behavior for correctness, but it means Redis availability matters a lot.

### Local Docker numbers are not production numbers

The current load numbers are useful for correctness validation, not for claiming production hot-path latency.

### Refund semantics intentionally require a real prior consume request

This is safer than arbitrary-unit refunds, but it also means callers must preserve the original consume idempotency key if they want to compensate later.

### The FastAPI routes are illustrative

The important part of the repository is the quota engine design and concurrency behavior, not the exact sample business endpoints.

Project Structure

```bash
quota-meter/
├── app/
│   ├── core/
│   │   └── config.py           # Redis + Mongo connection setup
│   ├── quota/
│   │   ├── models.py           # Request / response models
│   │   ├── store.py            # Redis Lua scripts + low-level store operations
│   │   ├── service.py          # Quota business logic
│   │   └── dependencies.py     # FastAPI quota guard / helpers
│   ├── features/
│   │   └── routes.py           # Sample feature + quota/admin routes
│   └── main.py                 # FastAPI app bootstrap
├── load_test/
│   └── simulate.py             # Load simulator
├── tests/
│   └── test_concurrency.py     # Concurrency / idempotency / refund tests
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── DESIGN.md
|__ README.md
```

## Engineering Scope and AI Usage

This project was built as a real engineering exercise rather than a pure code-generation exercise.

AI assistance was used as a development aid for things like:

- brainstorming design alternatives
- pressure-testing tradeoffs
- refining load-test scenarios
- tightening documentation structure
- generating complicated lua scripts

But the important implementation choices still required engineering judgment and validation:

- choosing Redis Lua for concurrency-safe quota deduction
- deciding on all-or-nothing batch semantics
- deciding to model refunds by original consume request idempotency key
- aligning the tests, load simulation, and docs to describe the same system
- validating the final behavior using real test and load outputs rather than placeholder claims

## Summary

Quota Meter is a small but concurrency-focused quota service that aims to be:

- correct under contention
- safe for retries
- explicit about failure semantics
- horizontally scalable at the application layer
- easy to run locally with a single Docker command

For deeper design rationale, tradeoffs, rejected alternatives, and corner-case handling, see DESIGN.md.
