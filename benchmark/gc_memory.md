# GC & Memory Tuning Benchmark Analysis

I have implemented and executed a comprehensive Go benchmark suite (`BenchmarkProcessNext_GCTuning` and `BenchmarkHandleTask_GCTuning`) for both the Producer and Consumer services. 

The tests used mocked API and database interactions, isolating the CPU and memory performance of the Go application logic itself under different garbage collection configurations.

## Benchmark Configuration
We tested various combinations of `GOGC` (the garbage collection target percentage) and `GOMEMLIMIT` (a soft memory cap that the runtime tries to respect by triggering GC more aggressively).

The benchmark runs isolated the core processing loop (`processNext` for Producer, `HandleTask` for Consumer) with no sleeping or rate limiting, effectively measuring raw throughput.

## Producer Results (M4 CPU)

| Scenario | GOGC | GOMEMLIMIT | Time/Op (ns) | Allocations/Op (B) |
| :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 100 | Unlimited | 13,422 | 12,290 |
| **Aggressive GC** | 50 | Unlimited | 13,943 | 12,304 |
| **Lazy GC** | 200 | Unlimited | 13,112 | 11,852 |
| **Very Lazy GC** | 500 | Unlimited | 13,390 | 12,222 |
| **Limit 64MB** | 100 | 64 MB | 35,239 | 12,716 |
| **Limit 32MB** | 100 | 32 MB | 51,459 | 14,801 |
| **Tight Limit** | 50 | 16 MB | 40,276 | 15,246 |

## Consumer Results (M4 CPU)

| Scenario | GOGC | GOMEMLIMIT | Time/Op (ns) | Allocations/Op (B) |
| :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 100 | Unlimited | 20,896 | 13,708 |
| **Aggressive GC** | 50 | Unlimited | 22,620 | 13,733 |
| **Lazy GC** | 200 | Unlimited | 20,550 | 13,436 |
| **Very Lazy GC** | 500 | Unlimited | 20,941 | 13,782 |
| **Limit 64MB** | 100 | 64 MB | 64,854 | 14,187 |
| **Limit 32MB** | 100 | 32 MB | 93,883 | 18,395 |
| **Tight Limit** | 50 | 16 MB | 60,026 | 16,812 |

## Key Takeaways

1. **GOGC Tuning Impact is Marginal on High Throughput:**
   - Increasing `GOGC` to `200` (allowing memory to grow 200% before a GC cycle) provided a **minor performance boost** (~2-3% faster). This is expected; GC runs half as often, so less CPU time is spent on mark-and-sweep.
   - Dropping `GOGC` to `50` (running GC twice as often) resulted in a slight performance penalty (~4-8% slower) as the CPU spent more cycles pausing and sweeping.

2. **GOMEMLIMIT Drastically Impacts CPU (Thrashing):**
   - As soon as a tight `GOMEMLIMIT` is introduced (e.g., 64MB or 32MB), the time per operation **plummets by 2x to 4x**. 
   - This occurs because the application is generating heavy allocation pressure (approx 12-14KB per task). When the runtime nears the memory limit, it begins desperately triggering GC cycles to free memory and stay under the cap. This GC "death spiral" consumes vast amounts of CPU, severely restricting actual application throughput.
   - For example, in the Consumer service, moving from Unlimited memory to a 32MB limit inflated the latency from `20µs` to `93µs`.

3. **Recommendation for Production:**
   - **Do not set a tight `GOMEMLIMIT` unless strictly necessary.** If the container is memory constrained, set `GOMEMLIMIT` to around 80-90% of the cgroup hard limit to prevent OOM Kills, but ensure the hard limit is generous enough that the application isn't constantly thrashing.
   - A `GOGC` of `100` is perfectly optimal for these services given their allocation patterns. If memory footprint is completely unconstrained and you wish to squeeze out absolute maximum CPU efficiency, `GOGC=200` is viable. 

This pure Go benchmark mirrors what you will observe in the Grafana dashboard under load: heavy memory limits will cause the "GC CPU Fraction" panel to spike, driving down your "Task Throughput" panel.

## Rate-Limited vs. Unlimited Matrix Analysis

We ran a matrix of tests multiplying `produce_rate_per_sec` (Producer) and `rate_limit_per_sec` (Consumer) with various GC tunings.

**Producer Matrix Highlights (M4 CPU):**

| Scenario | Rate (per sec) | GOGC | GOMEMLIMIT | Time/Op |
| :--- | :--- | :--- | :--- | :--- |
| **Rate20_GOGC100** | 20 | 100 | Unlimited | 50.9 ms |
| **Rate20_Limit64MB** | 20 | 100 | 64 MB | 50.9 ms |
| **Rate100_GOGC100** | 100 | 100 | Unlimited | 10.9 ms |
| **Rate1000_GOGC100** | 1000 | 100 | Unlimited | 1.17 ms |
| **RateUnlimited_GOGC100** | Unlimited | 100 | Unlimited | 13.4 µs |
| **RateUnlimited_Limit64MB** | Unlimited | 100 | 64 MB | 27.3 µs |

**Consumer Matrix Highlights (M4 CPU):**

| Scenario | Rate (per sec) | GOGC | GOMEMLIMIT | Time/Op |
| :--- | :--- | :--- | :--- | :--- |
| **Rate20_GOGC100** | 20 | 100 | Unlimited | 40.0 ms |
| **Rate20_Limit64MB** | 20 | 100 | 64 MB | 40.0 ms |
| **Rate100_GOGC100** | 100 | 100 | Unlimited | 9.9 ms |
| **Rate1000_GOGC100** | 1000 | 100 | Unlimited | 0.9 ms |
| **RateUnlimited_GOGC100** | Unlimited | 100 | Unlimited | 19.0 µs |
| **RateUnlimited_Limit64MB** | Unlimited | 100 | 64 MB | 43.2 µs |

### Matrix Key Takeaways:
1. **Low Rates Mask GC Issues**: When running at a bounded rate (e.g., 20 ops/sec, taking ~50ms/op intentionally due to sleeping/limiting), the GC overhead is completely masked. A 64MB tight memory limit caused zero performance degradation at 20 ops/sec or 100 ops/sec, because the idle CPU time gives the GC plenty of room to clean up memory without pausing application logic execution.
2. **High/Unlimited Rates Expose Thrashing**: Only when the rate limits are removed (`Unlimited`) does the application push memory allocations fast enough to hit the 64MB limit, triggering the GC death-spiral (Time/Op inflates from `13µs` to `27µs` on Producer and `19µs` to `43µs` on Consumer).
3. **Conclusion**: At your default configuration of `20 ops/sec`, GC parameters and `GOMEMLIMIT` will have negligible impact on latency or throughput. If you decide to scale these services to process thousands of tasks per second, configuring generous memory limits will become critical.


Configuration	Rate Unlimited (ns/op)	B/op	Allocs/op
GOGC 100	13,634	12,206	130
GOGC 50	14,108	12,316	130
64MB Limit	25,806	12,651	130