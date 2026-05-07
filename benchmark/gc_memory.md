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

Configuration	Rate Unlimited (ns/op)	B/op	Allocs/op
GOGC 100	13,634	12,206	130
GOGC 50	14,108	12,316	130
64MB Limit	25,806	12,651	130