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


## Concurrency Correctness

* Since we need near 0ms latency an in-memory data-store like Redis will come in handy
* However, I believe even with Redis (check + deducting credit one step at a time can be time consuming since two round trips for two commands, and also, a potential race condition)
  
```txt
for example: org 1 has 100 units remaining
instance 1 : get : sees 100 : decides grant
instance 2 : get : sees 100 : decides grant (counter is still 100 : not updated yet which is a gap)
instance 3 : get : sees 100 : decides grant (still 100!!)
Over-served from 100 to 300!! 
```

* A fix to this would be executing both check and deduction as a single atomic command