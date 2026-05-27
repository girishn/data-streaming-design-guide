# Broker Thread Configuration and Log Flush Tuning

## Broker Thread Architecture

A Kafka broker uses multiple dedicated thread pools to separate concerns. Understanding which pool handles which operation determines where to look when a broker is saturated.

```
Client connection → Network threads (socket I/O)
                         ↓
                    Request queue
                         ↓
                    I/O threads (request processing, disk)
                         ↓
                    Response queue
                         ↓
                    Network threads (send response)

Replication ← Replica fetcher threads (fetch from leader)
```

Each pool can become a bottleneck independently. A saturated network thread pool causes slow request reads without affecting I/O threads. A saturated I/O thread pool causes request queue depth to grow while network threads are idle. Monitor both thread pools separately.

## `num.network.threads`

**Default: 3**

Network threads handle socket I/O — reading bytes from incoming client connections and writing response bytes back. Each configured listener (except the inter-controller listener in KRaft) creates its own network thread pool. A broker with three listeners has three independent network thread pools, each with `num.network.threads` threads.

**When to increase:**
- High concurrent client connection count (thousands of connections per broker)
- TLS encryption enabled: TLS handshakes consume network thread time for the crypto negotiation phase. Under heavy connection churn (clients reconnecting frequently), TLS handshake overhead can saturate the network thread pool before I/O threads are stressed.
- `NetworkProcessorAvgIdlePercent` below 30% (see `12-Monitoring-Observability/broker-jmx-metrics.md`)

**Sizing guidance:**
- 3–6: standard deployments with moderate client counts
- 6–9: high client counts or TLS with frequent reconnects
- Beyond 9: rarely necessary; investigate root cause (connection churn, TLS session reuse disabled) before adding threads

Increasing network threads beyond the point of saturation does not improve throughput — it adds context-switching overhead. Profile before tuning.

## `num.io.threads`

**Default: 8**

I/O threads process requests dequeued from the request queue: produce requests (append to log), fetch requests (read from log), offset commits, metadata requests. These threads perform the actual disk reads and writes.

**When to increase:**
- Disk I/O parallelism: if the broker has multiple disks (`log.dirs` pointing to separate mount points), more I/O threads allow concurrent writes to different disks. A general heuristic is 2× the number of disks.
- `RequestHandlerAvgIdlePercent` below 30%: I/O threads are the request handlers; low idle percentage indicates saturation
- Broker-side schema validation enabled: validation requires decompressing and parsing record batches, which runs in I/O threads. Higher validation overhead requires more threads to maintain the same request throughput.

**Sizing guidance:**
- 8 (default): adequate for 1–2 disks, moderate traffic
- 16: 4+ disks, high produce/fetch throughput
- 32+: very high throughput with many disks; validate with `RequestHandlerAvgIdlePercent` before increasing further

Adding I/O threads beyond CPU core count causes context-switching overhead that can reduce throughput. Cap at roughly 2× available vCPUs as a starting point.

## `num.replica.fetchers`

**Default: 1**

Each follower replica maintains fetcher threads that pull data from the partition leader on other brokers. `num.replica.fetchers` controls how many fetcher threads a broker runs for its follower replicas.

With the default of 1, all follower fetch requests on a broker are serialised through a single thread. On a broker with hundreds of follower partitions across many leaders, this single thread may not keep pace with the combined produce rate, causing replicas to fall behind and triggering ISR shrinks.

**When to increase:**
- `IsrShrinksPerSec` is elevated without corresponding network or disk problems on the leader
- Follower brokers are consistently behind (`UnderReplicatedPartitions > 0`) despite having available CPU and network bandwidth
- High partition counts per broker (1000+) spread across many leaders

**Sizing guidance:**
- 1 (default): adequate for low–moderate partition counts
- 2–4: high partition counts or catching up after a lagging follower
- 4–8: very high partition counts; increasing beyond 4 provides diminishing returns and increases CPU/memory on both the follower (fetcher threads) and the leader (handling more concurrent fetch requests)

Increasing `num.replica.fetchers` increases memory on the leader proportional to the number of additional concurrent fetch sessions it must track. On a leader with many followers each using multiple fetcher threads, this can be significant.

## Log Flush Configuration

### The Default: Rely on OS Page Cache

Kafka's default `log.flush.interval.messages` and `log.flush.interval.ms` are set to `Long.MAX_VALUE` — effectively, never explicitly flush. Kafka does not call `fsync()` on log segments during normal operation.

Instead, durability is provided by **replication**. When a record is acknowledged with `acks=all`, it exists on at least `min.insync.replicas` brokers. Even if none of those brokers have called `fsync()`, the data is not lost as long as at least one broker survives — the in-memory page cache on a surviving broker contains the data, which the OS will flush to disk eventually or on clean shutdown.

This model works because:
1. Hardware failures that corrupt in-memory state (power loss without UPS, kernel panic) are rare relative to disk failures
2. Broker crashes typically result in a clean JVM exit, which flushes page cache
3. RF=3 with min.insync.replicas=2 provides two independent copies, making simultaneous catastrophic failure of both unlikely

### Do Not Configure Explicit Flushes

`log.flush.interval.messages=1` (flush after every record) or `log.flush.interval.ms=100` (flush every 100ms) reduces throughput by **two to three orders of magnitude** in benchmarks. The OS page cache batches writes and flushes them efficiently in the background. `fsync()` is a synchronous, blocking operation that waits for the disk controller to confirm the physical write — on spinning disks this takes 5–20ms per call; on NVMe it is faster but still blocks the I/O thread.

The only scenario where explicit flush configuration is appropriate:
- Single-node deployments with no replication (RF=1, testing/development environments)
- Regulatory requirements that mandate physical persistence before acknowledgment — confirm whether the requirement is actually for `fsync` or merely for durability guarantees (which replication already provides)

### `log.dirs` and Disk Layout

Multiple `log.dirs` entries (separate physical mount points) distribute partition data across disks, allowing parallel I/O from multiple I/O threads. This is more impactful than any thread tuning:

```properties
# Multiple disks — Kafka round-robins new partitions across these paths
log.dirs=/data/kafka-1,/data/kafka-2,/data/kafka-3,/data/kafka-4
```

Each directory should be on a separate physical disk or NVMe device — not separate partitions of the same disk. SSDs and NVMe significantly reduce the latency impact of `acks=all` because follower fetch I/O is faster, reducing the wait for ISR acknowledgments.

## Tuning Sequence

When a broker is saturated, diagnose before tuning:

1. Check `NetworkProcessorAvgIdlePercent` — if below 30%, increase `num.network.threads`
2. Check `RequestHandlerAvgIdlePercent` — if below 30%, increase `num.io.threads`
3. Check `IsrShrinksPerSec` and `UnderReplicatedPartitions` — if elevated without network/disk saturation, increase `num.replica.fetchers`
4. Check disk utilisation and I/O wait — if I/O is saturated, add disks to `log.dirs` or upgrade to faster storage before tuning threads
5. Check TLS impact — if TLS is enabled and network threads are saturated, consider AES-NI verification or per-listener TLS configuration

Thread tuning is a secondary lever. Storage and network capacity are the primary constraints. Adding threads to a disk-bound broker adds context-switching overhead without improving throughput.
