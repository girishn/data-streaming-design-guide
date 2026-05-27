# Producer Performance Tuning — Batching, Compression, and Durability

## The Throughput-Latency Tension

Every producer configuration decision trades throughput against latency. Throughput improves when records are grouped into larger batches — fewer network round trips, better compression ratios, lower broker request-handling overhead per record. Latency decreases when records are sent immediately — no waiting, no buffering. These goals conflict, and the right balance depends on the workload.

A payment event that drives a customer-visible action needs low latency. A metrics pipeline ingesting telemetry from thousands of sensors needs throughput. Most workloads sit between these extremes and benefit from tuning that leans toward one without fully sacrificing the other.

## Batching: `batch.size` and `linger.ms`

The producer accumulates records destined for the same partition into a batch before sending. Two parameters control batch formation:

**`batch.size`** (default: 16,384 bytes / 16 KB)

The maximum size of a batch in bytes. When a batch reaches this size, it is sent immediately regardless of `linger.ms`. The producer pre-allocates a buffer of this size for every active partition — a producer writing to 100 partitions with `batch.size=1MB` holds 100 MB of buffer in the producer's `buffer.memory`.

Practical values:
- 16 KB (default): appropriate for low-latency, moderate-throughput workloads
- 64 KB – 256 KB: high-throughput pipelines where batching efficiency matters
- 1 MB: maximum batching for bulk ingestion scenarios; requires `buffer.memory` to be sized accordingly

**`linger.ms`** (default: 5ms as of Kafka 4.0; was 0 in earlier versions)

An artificial delay before sending a batch that has not yet reached `batch.size`. The producer waits up to `linger.ms` to accumulate more records. If `linger.ms=0`, batches are sent as soon as a record is available — effective for latency-sensitive workloads but produces many small batches under moderate load.

Practical values:
- 0: minimum latency; each record sent as soon as it arrives (or micro-batched with concurrent sends)
- 5–20ms: balanced; most high-traffic producers will fill batches before the linger expires
- 50–100ms: throughput-optimised; meaningful latency floor but significantly larger batches

**Interaction:** `linger.ms` only adds latency when the producer is not filling batches organically. Under sustained high throughput, batches reach `batch.size` before `linger.ms` expires and the linger cost is zero. `linger.ms` tuning matters most at moderate load where batches would otherwise be sent half-empty.

## Compression

Compression is applied to the entire batch on the producer before sending. Larger batches produce better compression ratios — this is another reason to tune `batch.size` and `linger.ms` upward for throughput workloads.

Compressed batches are stored compressed on the broker and decompressed by the consumer. The broker does not decompress for storage — except when broker-side schema validation is enabled, which requires decompression to inspect record contents. See `08-Stream-Governance/broker-side-validation.md`.

**Codec comparison:**

| Codec | CPU cost | Compression ratio | Best for |
|---|---|---|---|
| `none` | Zero | 1x | Latency-critical, already-compressed payloads |
| `lz4` | Low | Moderate | High-throughput with CPU budget constraints |
| `snappy` | Low | Moderate | High-throughput; good default for most pipelines |
| `zstd` | Medium–High | High | Storage-constrained topics; batch ingestion |
| `gzip` | High | High | Legacy compatibility; avoid for new deployments |

`lz4` and `snappy` are the practical defaults for production pipelines. `zstd` is worth considering when storage costs are a constraint and CPU headroom exists. `gzip` is slower than `zstd` at comparable ratios — prefer `zstd` for new deployments.

**Enabling compression:**
```properties
compression.type=lz4
batch.size=262144        # 256 KB — larger batches improve compression ratio
linger.ms=20
```

## Durability: `acks` Settings

`acks` controls when the broker considers a produce request complete and sends acknowledgment to the producer:

**`acks=0`:** The producer fires and forgets — it does not wait for any acknowledgment from the broker. The broker may not have received the record at all if it crashed between the send and the network write. Highest throughput, zero durability guarantee. Use only for non-critical telemetry or logging where occasional loss is acceptable.

**`acks=1`:** The leader broker acknowledges once it has written the record to its local log, without waiting for followers to replicate. If the leader crashes immediately after acknowledging but before replication, the record is lost. Middle ground between throughput and durability.

**`acks=all` (or `-1`):** The leader waits for all in-sync replicas (ISR) to acknowledge before responding. A record acknowledged with `acks=all` is durable as long as at least one ISR survives. This is the only configuration that prevents data loss under normal failure conditions.

Performance cost of `acks=all`: the round trip now includes the time for followers to fetch from the leader and acknowledge. Under normal conditions with healthy followers on the same network, this adds 1–5ms. Under follower pressure (disk I/O saturation, GC pauses, network congestion), p99 latency can spike significantly. Throughput can drop 20–40% compared to `acks=1` when followers are slow.

Production default for critical data:
```properties
acks=all
enable.idempotence=true      # requires acks=all; prevents duplicates on retry
retries=2147483647           # Integer.MAX_VALUE
max.in.flight.requests.per.connection=5   # maximum with idempotence enabled
```

## TLS Throughput Impact

TLS encryption breaks Kafka's zero-copy sendfile path. Without TLS, the broker sends log segment data directly from the OS page cache to the network socket without copying through userspace — extremely efficient. With TLS, the data must pass through the JVM for encryption before it reaches the network layer, requiring a copy through userspace on every send.

This architectural constraint reduces producer and consumer throughput by 20–40% compared to plaintext, independent of CPU speed. Modern CPUs with AES-NI hardware acceleration minimise the encryption computation itself, but the copy overhead remains.

Account for this in capacity planning: a cluster sized for 500 MB/s plaintext throughput delivers roughly 300–400 MB/s under TLS. Do not discover this gap in a production incident.

Mitigation options:
- Hardware with AES-NI (standard on all modern x86 server CPUs)
- TLS termination at a load balancer for specific listener paths (client-facing only), keeping broker-to-broker replication on an internal plaintext listener

## Configuration Reference

| Scenario | `batch.size` | `linger.ms` | `compression.type` | `acks` |
|---|---|---|---|---|
| Minimum latency | 16 KB | 0 | none | 1 |
| Balanced (most production) | 64–256 KB | 5–20 | lz4 or snappy | all |
| Maximum throughput | 512 KB–1 MB | 50–100 | lz4 or zstd | 1 |
| Maximum durability | 64–256 KB | 5–20 | lz4 or snappy | all + idempotence |
| Bulk ingestion | 1 MB | 100 | zstd | 1 or all |
