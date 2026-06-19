# Quota Meter

-> Here's my understand of the problem and my design decisions I've taken before jumping on to coding.

## Storage Design

* Redis will own the live counters
  * key might look like
  
  ```bash
  quota:{orgId}:{apiId}:{yyyy-mm}
  ```

* Our primary database will be the ultimate source of truths, it'll hold the quota limits/configs
  
  ```js
  {
    orgId: {
      apiId_1: credit_count1,
      apiId_2: credit_count2,
      apiId_3: credit_count3,
      ...
      apiId_30: credit_count
    } 
  }
  ```
  
* Counters never actually touch Primary DB - Redis only and conversely config will never be originated from Redis (Primary DB should always be source of truth)

## Concurrency Correctness Decisions

* Since we need near 0ms latency an in-memory data-store like Redis will come in handy
* However, I believe even with Redis (check + deducting credit one step at a time can be time consuming since two round trips for two commands, and also, a potential race condition)
  
```txt
for example: org 1 has 100 units remaining
instance 1 : get : sees 100 : decides grant
instance 2 : get : sees 100 : decides grant (counter is still 100 : not updated yet which is a gap)
instance 3 : get : sees 100 : decides grant (still 100!!)
Over-served from 100 to 300!! 
```

* A fix to this would be executing both check and deduction as a single atomic command, Redis can do this by executing LUA Scripts as a single atomic unit, no othe command in between. (No more distributed lock management overhead)

## Cache Architecture (most critical section)

### Cache-Aside ?

* Simple and easy to manage for low-traffic !! But for 10k TPS, this could lead to potential thundering herd problem !!
* for any reason if a key for an organisation gets evicted and a batch arrives for the same key:
  * Redis returns None (cache miss) for all
  * All the queries in the batch fire simultaneously to the Primary db
  * primary db returns same result to all task
  * all tasks end up attempting to compute the remaining balance and write back to redis

`A Fix`:

* Lazy loading config from primary db into redis on first access
* admin has to maintain both primary db and redis for write-through on config updates
* No bulk startup sync (reading an entire library at once before helping one customer with simple problem)

`Case where cold-starts can cause downtime`

* In cases of extreme cold-starts, first access per org per feature will cause some spike in latency due to a primary db read! breaching the budget of 10ms!!
* However, with proper indexing and connection pooling, this can be mitigated to some extent. (But then connection pooling will be another bottleneck requiring horizontal scaling of the primary db itself)

`On Another thought`

> What if we pre-load all config just for once into Redis ??

* With TTL None (permanent, no-expiration)
* Config change in primary db ? write-through updates both primary db + redis
* new org onboard ? write to primary db + redis immediately

> What changes with my current architecture ?

#### before

* Cache-aside(lazy - loading) + mutex + thundering herd concerns

#### after

* redis always has everything -> always a hit -> no primary db in hot path ever!!
* gaps ? when redis goes down/restart mode. (AI - Suggested concern and improvement tip)
  * We can either re-run the redis warmup script on redis restart (not production grade solution)
  * Redis saves snapshots to disk (on restart -> reload from disk)

### Revised Setup

* Primary DB (Mongo/Postgre) -> source of truth for configs
* redis -> permanent config cache + live counters
* hot path -> redis only, primary db remains untouched except new config updates

| Scenario | Before | After |
| --- | --- | --- |
| Cold Start | Thundering herd risk | With Warmup script, redis always ready to serve |
| Redis Restart ? | Mutex + lazy-load | Re-run warmup script or Use Redis RDB persistance (AI) |
| Config-change | Write-through | write-through (unchanged)
| new org added | lazy-load | write to both immediately

### Memory tradeoff

* Redis memory : 50k organisation * 30 features = 1.5M Keys
* Each key ~ 100 bytes -> ~150 MB in total
* Within Redis's capacity (which is typically 8-32GB)
* Nothing concerning here (can scale well too)

## Batch Behaviour?

* All or nothing (No Partial fulfilment)
* it's a supply-chain operation : partial container tracking is sort of meaningless

## Failure & Retries

* Idempotency in picture
  * First request ? idempotency key not seen -> execute lua -> cache result in redis (with some ttl)
  * retry request ? Idempotency key seen ? return cached result -> quota remains untouched
*
