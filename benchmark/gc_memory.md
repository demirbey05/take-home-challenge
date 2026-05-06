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
