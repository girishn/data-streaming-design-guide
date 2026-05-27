# Tiered Storage — Long Retention Without Local Disk Overhead

## What It Is

Tiered storage decouples compute and storage in Kafka by introducing a two-tier log architecture: recent data stays on broker-local disk (the **hotset**), and older segments are automatically migrated to remote object storage (S3, GCS, Azure Blob). Consumers see a single unified log regardless of which tier a segment physically lives on — the broker fetches from remote storage transparently.

Without tiered storage, long retention is bounded by local disk capacity. A 7-year retention requirement on a high-throughput topic is not economically viable on SSD-backed broker nodes. Tiered storage moves the cost curve: local SSD holds days of data; object storage holds years.

**Availability:**
- **Confluent Platform:** available since Confluent Platform 5.4, backed by S3, GCS, or Azure Blob
- **OSS Apache Kafka:** native tiered storage landed in Kafka 3.6.0 (KIP-405) as a production-ready feature
- **Confluent Cloud:** branded as **Infinite Storage**, built into the Kora engine — no configuration required, scales elastically

## Architecture

```
Producers → Broker (local SSD)
                │
                │  hotset window expires
                ▼
           Object Storage (S3 / GCS / Azure Blob)
                │
                │  consumer fetch (older segments)
                ▼
           Remote Log Manager (broker fetches transparently)
```

The **Remote Log Manager (RLM)** runs as a background process on the broker. It handles segment upload, lifecycle management, and fetch serving for cold segments. The critical property: from the consumer's perspective, the log is contiguous. Offset 0 through the current LEO is readable regardless of whether a segment is local or remote.

Tiered storage does **not** replace replication. HA is still governed by `replication.factor` and `min.insync.replicas`. Tiered storage and ISR replication are independent concerns — a committed message in remote storage is still subject to the normal durability guarantee: safe as long as at least one in-sync replica remains alive.

## Key Configuration (Confluent Platform)

**Hotset boundaries — when data moves from local to remote:**

```properties
# Move segments older than 1 day to remote storage (default)
confluent.tier.local.hotset.ms=86400000

# Also cap local size regardless of age
confluent.tier.local.hotset.bytes=107374182400  # 100 GB
```

Both are evaluated independently — whichever threshold is crossed first triggers migration. In practice, size-based eviction matters on high-throughput topics where age alone would leave too much data local.

**Throughput throttling — prevent migration from saturating the broker:**

```properties
# Cap upload rate to remote storage (bytes/sec)
remote.log.manager.copy.max.bytes.per.second=104857600  # 100 MB/s

# Cap fetch rate from remote storage for consumer reads
remote.log.manager.fetch.max.bytes.per.second=104857600  # 100 MB/s
```

Without these, a broker recovering from a backlog of un-migrated segments can generate sustained object storage traffic that competes with producer and consumer I/O. Throttle to a fraction of available network bandwidth.

**Thread pool tuning:**

```properties
# Threads for uploading segments and GC of deleted records
confluent.tier.archiver.num.threads=8

# Threads for serving consumer fetches from remote storage
confluent.tier.fetcher.num.threads=8
```

Increase fetcher threads if cold-tier consumer reads (historical replay, audit queries) are latency-sensitive. Archiver threads primarily affect migration throughput, not read latency.

**Enabling tiered storage on a topic:**

```properties
# On the topic
confluent.tier.enable=true
# Inherit cluster-level hotset config, or override per-topic
confluent.tier.local.hotset.ms=3600000  # 1 hour hotset for high-volume topics
```

## Retention Policy Interaction

Tiered storage extends where data lives, not how long. Topic `retention.ms` and `retention.bytes` still govern the total retention window — segments beyond the retention boundary are deleted from remote storage just as they would be from local disk.

The operational change: setting `retention.ms` to 7 years is now economically viable. Local disk holds the hotset (days); object storage holds everything older at a fraction of the cost.

`cleanup.policy=delete` is the correct policy for time-series data (append-only, full history required). Do not use `compact` for time-series topics — compaction keeps only the latest record per key, destroying historical readings.

For compacted topics that also benefit from tiered storage (e.g. large changelog topics):

```properties
cleanup.policy=compact
confluent.tier.cleaner.enable=true
```

## When to Use Tiered Storage

| Scenario | Use tiered storage |
|---|---|
| Retention requirement > available local disk capacity | Yes |
| Regulatory retention (7 years, financial, healthcare) | Yes |
| Historical replay for new consumers (catch-up from months ago) | Yes |
| Real-time low-latency processing only (hotset always sufficient) | Not necessary |
| Development / test clusters | No |

## Tiered Storage vs Kafka Log Compaction

These are not alternatives — they address different problems:

- **Tiered storage:** where old segments live (local vs remote)
- **Log compaction:** what records are retained (all vs latest-per-key)

They can be combined. A compacted changelog topic with tiered storage enabled keeps only the latest value per key but can store that compacted log across petabytes in object storage rather than on local disk.

## OSS Kafka (KIP-405) vs Confluent Platform

| | OSS Kafka 3.6+ | Confluent Platform |
|---|---|---|
| Availability | GA since 3.6.0 | GA since CP 5.4 |
| Remote backends | S3, GCS, Azure Blob (via plugin) | S3, GCS, Azure Blob |
| Hotset config | `remote.log.storage.system.enable`, `local.retention.ms` | `confluent.tier.*` properties |
| Operational maturity | Newer, less battle-tested | Mature, Confluent-supported |
| License | Apache 2.0 | Confluent Platform license |

For on-premises deployments without a Confluent license, OSS Kafka 3.6+ tiered storage is viable. Size the local retention window conservatively and throttle migration throughput as described above — the operational tuning guidance applies to both implementations.

See `02-Broker-Infrastructure/partitioning-strategies.md` for retention policy interaction with partition design, and `12-Multi-Region-DR/cluster-linking.md` for how tiered storage at the central cluster interacts with DR replication topologies.
