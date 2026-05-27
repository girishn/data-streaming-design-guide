# Consumer Fetch Tuning — Throughput, Latency, and Memory

## How Consumer Fetching Works

The Kafka consumer client fetches data from brokers in the background, independent of the application's `poll()` calls. The fetch layer sends `FetchRequest`s to each broker that leads partitions assigned to the consumer, receives batches of records, and stores them in an internal fetch buffer. When the application calls `poll()`, records are drained from this buffer and returned to the application — up to `max.poll.records` at a time.

This architecture means `max.poll.records` does not control how much data is fetched from the broker. It only controls how many records the application sees per `poll()` call. The consumer continues fetching in the background regardless.

Understanding this separation is the prerequisite for tuning: fetch parameters control the network layer; `max.poll.records` controls the application layer.

## Fetch Parameters: `fetch.min.bytes` and `fetch.max.wait.ms`

These two parameters control the broker's behaviour when it receives a `FetchRequest`:

**`fetch.min.bytes`** (default: 1 byte)

The broker will not respond to a fetch request until at least this many bytes are available across the requested partitions. With the default of 1 byte, the broker responds immediately with whatever data is available — even a single record. This minimises latency but generates many small fetch responses under moderate load.

Raising `fetch.min.bytes` tells the broker to wait for more data to accumulate before responding, reducing fetch round trips and producing larger, more efficient responses.

**`fetch.max.wait.ms`** (default: 500ms)

The maximum time the broker waits for `fetch.min.bytes` to be satisfied. If insufficient data is available after this duration, the broker responds with whatever it has. This is the latency ceiling for consumers on low-traffic topics.

**Interaction:** The broker responds when *either* condition is met — `fetch.min.bytes` available, or `fetch.max.wait.ms` elapsed. These parameters only add latency when the topic is not producing enough data to satisfy `fetch.min.bytes` within `fetch.max.wait.ms`.

| Tuning goal | `fetch.min.bytes` | `fetch.max.wait.ms` |
|---|---|---|
| Minimum latency | 1 (default) | 50–100ms |
| Balanced | 1 KB–64 KB | 500ms (default) |
| Maximum throughput | 1 MB+ | 500ms |

For a high-throughput pipeline where the topic is always producing, raising `fetch.min.bytes` to 1 MB has no latency cost — the broker fills the response immediately. For a low-traffic topic, `fetch.min.bytes=1MB` would introduce up to `fetch.max.wait.ms` of latency on every fetch cycle.

## `max.partition.fetch.bytes`

**Default: 1,048,576 bytes (1 MB)**

The maximum data the broker returns per partition in a single fetch response. The total data in a fetch response can be up to `max.partition.fetch.bytes × number_of_assigned_partitions`.

The consumer client allocates fetch buffer memory proportional to this value and the number of assigned partitions:

```
approximate fetch buffer memory ≈ max.partition.fetch.bytes × assigned_partition_count
```

A consumer assigned 64 partitions with `max.partition.fetch.bytes=1MB` allocates approximately 64 MB for fetch buffers. In a consumer group with many partitions per instance, this becomes a significant memory constraint.

Raising `max.partition.fetch.bytes` allows the consumer to retrieve larger batches per partition per request, improving throughput for partitions with high write rates. The tradeoff is increased heap pressure — size JVM heap (`-Xmx`) accordingly and monitor GC pause frequency.

Also note: `max.partition.fetch.bytes` must be at least as large as the largest record (or batch, if compression is enabled) on the topic. If a producer writes a batch larger than `max.partition.fetch.bytes`, the consumer cannot fetch it and will stall.

## `max.poll.records`

**Default: 500**

The maximum number of records returned per `poll()` call. This parameter operates entirely in the application layer — it does not affect how many records are fetched from the broker. The consumer fetches data in background threads and caches it; `max.poll.records` controls how many cached records are handed to the application at a time.

The primary purpose of `max.poll.records` is to keep processing time per `poll()` cycle within `max.poll.interval.ms`. If processing 500 records takes 25 seconds and `max.poll.interval.ms=30000`, the consumer has a 5-second margin before the broker considers it dead. If processing 500 records takes 45 seconds, the broker evicts the consumer mid-cycle and triggers a rebalance.

**Tuning approach:**
1. Measure average processing time per record in your application
2. Set `max.poll.records` so that `records × avg_processing_time` is well under `max.poll.interval.ms` — target 50–70% utilisation
3. If processing is fast and throughput is the goal, raising `max.poll.records` reduces `poll()` call overhead

```properties
# Example: processing takes ~10ms per record, interval is 30s
# Safe: 500 records × 10ms = 5s, well within 30s
max.poll.records=500
max.poll.interval.ms=30000
```

For streaming processors that handle each record in microseconds, `max.poll.records` can be raised to 5000–10000 to reduce poll call overhead. For consumers that make synchronous external calls per record, lower it to 10–50.

## `fetch.max.bytes`

**Default: 52,428,800 bytes (50 MB)**

The maximum total data the consumer fetches from the broker per request (across all partitions). This is a ceiling on the entire fetch response. Raising `max.partition.fetch.bytes` past `fetch.max.bytes / partition_count` has no effect. Ensure `fetch.max.bytes` is set appropriately when `max.partition.fetch.bytes` is raised for high-partition workloads.

## Memory Budget

Consumer fetch tuning has a direct memory cost. Before raising fetch parameters, calculate the heap budget:

```
fetch buffer ≈ max.partition.fetch.bytes × assigned_partitions
record buffer ≈ max.poll.records × avg_record_size
total consumer heap ≈ fetch buffer + record buffer + application overhead
```

For a consumer assigned 32 partitions with `max.partition.fetch.bytes=2MB` and `max.poll.records=2000` at 5KB average record size:
- Fetch buffer: 32 × 2MB = 64 MB
- Record buffer: 2000 × 5KB = 10 MB
- Total fetch layer: ~74 MB before application overhead

Set `-Xmx` with adequate headroom above this estimate to avoid GC pressure from fetch buffer allocations.

## Configuration Reference

| Scenario | `fetch.min.bytes` | `fetch.max.wait.ms` | `max.partition.fetch.bytes` | `max.poll.records` |
|---|---|---|---|---|
| Low-latency, low-volume | 1 | 100 | 1 MB | 100–500 |
| Balanced production | 1 KB | 500 | 1 MB | 500 |
| High-throughput pipeline | 1 MB | 500 | 4–8 MB | 2000–5000 |
| Slow per-record processing | 1 | 500 | 1 MB | 10–50 |
| Bulk replay / backfill | 10 MB | 500 | 10 MB | 5000–10000 |
