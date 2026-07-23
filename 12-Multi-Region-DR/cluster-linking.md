# Cluster Linking — Native Cross-Cluster Replication

## How It Works

Cluster Linking is a Confluent Server feature that extends the Kafka replica fetcher protocol across cluster boundaries. In standard Kafka replication, a follower replica on the same cluster fetches from the partition leader to stay in sync. Cluster Linking applies the same mechanism across clusters: the leader of a **mirror topic** on the destination cluster fetches directly from the leader of the source topic on the source cluster.

This architectural choice has a critical consequence: mirror topics are **byte-for-byte replicas** of their source topics. The same bytes, the same offsets, the same timestamps. Offset 1000 on the source is offset 1000 on the destination. This is fundamentally different from MirrorMaker 2, which re-produces records and generates new offsets on the destination.

No Kafka Connect framework is required. The replication runs as an internal broker process, not as an external application.

**Cluster Link creation:**
```bash
# Create a link from destination cluster pointing to source
confluent kafka link create my-dr-link \
  --source-cluster-id <source-cluster-id> \
  --source-bootstrap-server broker.source.confluent.cloud:9092 \
  --source-api-key <source-api-key> \
  --source-api-secret <source-api-secret>

# Create a mirror topic on the destination
confluent kafka mirror create orders \
  --link my-dr-link
```

The mirror topic `orders` on the destination now continuously replicates from `orders` on the source. Producers write to the source; the destination mirror stays within seconds of the source's LEO.

## Offset Parity

Because mirror topics are byte-for-byte replicas, offset parity is an inherent property — not a configuration option. This has direct operational implications:

- A consumer group at offset 5000 on the source can resume from offset 5000 on the destination after failover with no translation step
- Consumers do not need to be aware they have failed over — the data at offset 5000 is identical
- Schema IDs embedded in record bytes are preserved, so Schema Registry must also be replicated or accessible from both regions

Compare this with MirrorMaker 2, where the offset on the destination is a different number from the source offset, requiring explicit translation at failover time. See `12-Multi-Region-DR/mirrormaker2.md`.

## Consumer Group Offset Synchronisation

Cluster Linking can synchronise consumer group committed offsets from source to destination. When enabled, the destination cluster maintains a continuously updated copy of each consumer group's committed offsets, translated to the destination's offset space (which is identical to the source's offset space due to byte-for-byte parity).

```bash
# Enable consumer offset sync on the link
confluent kafka link update my-dr-link \
  --config consumer.offset.sync.enable=true \
  --config consumer.offset.sync.ms=30000   # sync every 30 seconds
```

With offset sync enabled, a consumer group that has committed offset 5000 on the source will have offset 5000 committed on the destination. At failover, the group resumes from exactly where it stopped on the source — within the `consumer.offset.sync.ms` window.

## Failover and Promotion

Cluster Linking provides two distinct commands for DR transitions:

**`failover`** — used when the source cluster is unavailable (disaster scenario):
```bash
confluent kafka mirror failover orders --link my-dr-link
```
This transitions the mirror topic from `MIRROR` state (read-only) to `ACTIVE` state (writable). Consumer offsets are clamped to the destination's log end offset — if any records were in-flight at the time of the source failure and were not yet replicated, those offsets are set to the end of what was replicated, preventing consumers from getting stuck on offsets that don't exist on the destination.

**`promote`** — used for planned migrations (source is still available):
```bash
confluent kafka mirror promote orders --link my-dr-link
```
Promote waits for the mirror to fully catch up to the source LEO before transitioning to `ACTIVE`. This ensures zero data loss for planned cutovers. After promotion, the mirror topic is fully independent — no ongoing link to the source.

After failover or promotion, update producer and consumer bootstrap configurations to point to the destination cluster. Cluster Linking does not redirect clients automatically.

## What Gets Synchronised

By default, Cluster Linking replicates topic data only. Additional synchronisation options:

| Configuration | Default | Effect |
|---|---|---|
| `topic.config.sync.ms` | `5000` (enabled) | Syncs topic-level configurations (retention, compaction, segment size) — on by default |
| `acl.sync.enable` | `false` | Syncs topic ACLs to destination |
| `consumer.offset.sync.enable` | `false` | Syncs consumer group committed offsets |
| `consumer.offset.sync.ms` | `30000` | How often offsets are synced (ms) |

Topic config sync is already on by default; explicitly enable ACL sync and offset sync for a complete DR setup. Without ACL sync, consumers on the destination will lack the permissions they had on the source. Without offset sync, consumers must restart from the beginning or latest offset.

## Confluent Cloud vs Confluent Platform

**Confluent Cloud:** Cluster Linking is available on Dedicated and Enterprise cluster types. The link is created through the Confluent CLI or Cloud API. The source can be another Confluent Cloud cluster or a self-managed Kafka cluster (with a compatible Confluent Server broker on the source side).

**Confluent Platform:** Cluster Linking requires Confluent Server (not OSS Apache Kafka) on both source and destination. It is included in the Confluent Platform license.

**Limitation:** Cluster Linking is a Confluent-proprietary feature. If either the source or destination cluster runs OSS Apache Kafka without Confluent Server, Cluster Linking is not available — use MirrorMaker 2 instead.

## When to Choose Cluster Linking

- DR requirement with near-zero RPO and second-scale RTO
- Consumer groups must resume exactly where they stopped after failover (offset parity is essential)
- Both clusters run Confluent Server or Confluent Cloud Dedicated/Enterprise
- Preference for operational simplicity over framework flexibility — no Connect infrastructure to manage

For open-source Kafka, tolerance for offset translation, or requirements for active-active bidirectional replication with conflict resolution, see `12-Multi-Region-DR/mirrormaker2.md` and `12-Multi-Region-DR/active-active-dr.md`.
