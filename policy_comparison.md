# Northwind Labs scheduling policy comparison

**Fair cost $3,038.77. Priority FIFO cost $3,039.74.** Fair saved $0.97 (0.03204%). Every team's p50 and p95 wait matched. Platform's p99 wait was 2 minutes 11 seconds shorter under Fair. The other p99 values matched. [Source replay results](policy_comparison.json)

## How the policies work

| Policy | Queue order |
|---|---|
| **Fair** | Uses effective priority first. When priorities tie, it favors the team using fewer running or reserved GPUs, then the earliest submission. A waiting job gains one priority level every 30 simulated minutes. After an hour, the oldest job without a reservation can have capacity held for it. |
| **Priority FIFO** | Considers high, then normal, then low priority. Within a priority, the earliest submission goes first. FIFO means first in, first out. |

Both policies prefer a ready node and choose the tightest fit. They can reserve GPUs on a booting node or launch the smallest suitable node when needed.

Suppose Vision already uses 8 GPUs and Speech uses zero. Both have a newly queued normal-priority job that fits the available slot, and Vision submitted first. Fair considers Speech first because that team uses fewer GPUs. Priority FIFO considers Vision first because its job arrived earlier.

## Wait times by team

Wait runs from submission to first start. Each policy cell shows **p50 / p95 / p99** as minutes and seconds. For example, `97:00` means 97 minutes.

| Team | Jobs started per policy | Fair wait | Priority FIFO wait |
|---|---:|---:|---:|
| Forecast | 64 | 0:00 / 6:33 / 7:48 | 0:00 / 6:33 / 7:48 |
| NLP | 80 | 0:57 / 7:47 / 19:24 | 0:57 / 7:47 / 19:24 |
| Platform | 24 | 0:00 / 87:13 / 97:00 | 0:00 / 87:13 / 99:11 |
| Speech | 56 | 5:28 / 12:47 / 73:06 | 5:28 / 12:47 / 73:06 |
| Vision | 255 | 35:06 / 122:27 / 128:04 | 35:06 / 122:27 / 128:04 |

## Usage and cost by team

Each team used the same GPU-hours under both policies. Dollar amounts use the precision recorded in the JSON.

| Team | GPU-hours per policy | Fair cost (USD) | Priority FIFO cost (USD) |
|---|---:|---:|---:|
| Forecast | 167.58333333 | $338.884647 | $338.951389 |
| NLP | 365.65000000 | $560.141468 | $560.320146 |
| Platform | 192.85000000 | $406.017664 | $406.447353 |
| Speech | 985.60000000 | $1,125.058375 | $1,124.852916 |
| Vision | 381.41666667 | $608.666457 | $609.170835 |
| **Total** | **2,093.10000000** | **$3,038.768611** | **$3,039.742639** |

## Job and provider outcomes

| Metric | Fair | Priority FIFO |
|---|---:|---:|
| Jobs started | 479 | 479 |
| Jobs never started | 0 | 0 |
| Succeeded jobs | 432 | 432 |
| Failed jobs | 30 | 30 |
| Cancelled jobs | 17 | 17 |
| Accepted node launches | 129 | 129 |
| Terminated nodes | 127 | 127 |
| Nodes that failed to boot | 2 | 2 |
| Provider capacity rejections | 25 | 25 |

## Replay settings

| Setting | Value |
|---|---|
| Provider seed | `42` for both policies |
| Ready-node idle timeout | 300 simulated seconds (5 minutes) |
| Cost window start, inclusive | 2025-03-03 01:57:00 UTC |
| Cost window end, exclusive | 2025-03-09 19:04:02 UTC |
| Simulated time at report | 2025-03-09 19:04:01 UTC |

This is a comparison on one trace and one provider seed.

## Trace audit

The source contained **494 data rows**. The replay accepted **479**, skipped **13** exact repeats, and rejected **2** unsupported GPU requests. The duplicate CSV lines were 42, 117, 147, 160, 189, 194, 226, 228, 286, 290, 360, 453, 464.

| Rejected CSV line | Reason |
|---:|---|
| 88 | gpus must be 1, 2, 4, or 8 |
| 393 | gpus must be 1, 2, 4, or 8 |
