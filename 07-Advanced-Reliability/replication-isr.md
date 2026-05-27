# Replication and ISR — Durability Mechanics

## How Replication Works

Kafka replication operates at the partition level on a leader-follower model. Each partition has one leader broker and zero or more follower replicas, determined by the replication factor.

**The leader** handles all producer writes and consumer reads for a partition. It is the single point of truth for what is committed.

**Followers** replicate by sending periodic fetch requests to the leader — the same API used by consumers. Followers do not serve client traffic. They maintain a local copy of the partition log and advance their log end offset as fetch responses arrive.

**Committed vs uncommitted records:** a record is considered committed only after every member of the In-Sync Replicas (ISR) set has written it to their local log. The leader tracks the **High Watermark (HWM)** — the highest offset that has been replicated to all ISR members. Consumers with default isolation only read up to the HWM, not the log end offset. A producer with `acks=all` receives an acknowledgment only after the HWM advances past the written record.

## The ISR Set

The ISR is the set of replicas the leader considers sufficiently caught-up to be trusted for a write acknowledgment.

**ISR qualification:** a replica is in-sync if it meets both conditions:
1. Has an active session with the cluster controller
2. Has fetched up to the leader's HWM within the `replica.lag.time.max.ms` window (default 30s in Confluent Platform, 10s in some OSS configurations)

**ISR shrinkage:** a replica is removed from the ISR when it fails to fetch within the lag window. Common causes:
- Broker failure or restart
- Network partition between broker and leader
- Disk I/O saturation causing fetch delays
- Long GC pause on the follower JVM

When a replica falls out of the ISR, it becomes an **Out-of-Sync Replica (OSR)**. It continues fetching and will be re-added to the ISR once it catches up to within the lag window.

**`min.insync.replicas` (MISR) enforcement:** with `acks=all`, the broker rejects write requests with `NotEnoughReplicasException` when the live ISR size drops below `min.insync.replicas`. This is the durability floor — the cluster refuses to acknowledge writes that cannot be replicated to the minimum required set.

```
RF=3, min.insync.replicas=2:
- 3 brokers alive → normal operation
- 1 broker down (ISR=2) → writes continue, RF degraded, alert required
- 2 brokers down (ISR=1) → writes rejected, partition effectively read-only
```

Setting `min.insync.replicas = RF` means any broker failure stops writes entirely — maximum durability, minimum availability. Setting it to 1 degrades to at-least-once on a single replica — avoid.

## Leader Epoch and Log Divergence Prevention

The **leader epoch** is a 32-bit monotonically increasing counter associated with each partition. It is stored in the `leader-epoch-checkpoint` file on each broker and incremented every time a new leader is elected for a partition.

**The divergence problem it solves:** consider a leader that writes records at offsets 100–105 and sends ACKs to producers, but these records have not yet been replicated to followers. The leader crashes. A follower with only offsets 0–99 is elected as the new leader. When the old leader recovers and tries to rejoin as a follower, it has records at offsets 100–105 that the new leader does not. Without leader epoch tracking, both logs would be considered valid and the divergence would persist.

**How leader epoch prevents this:** when the old leader recovers, it contacts the new leader and asks: "What is the last offset you have for my previous epoch?" The new leader responds with the offset where the old epoch ended from its perspective. The recovering broker truncates its log to that offset, discarding the unacknowledged records that were never replicated. The logs converge.

This mechanism ensures that records which were in the old leader's log but not in the ISR are treated as if they were never committed — even if the producer received an ACK (only possible if `acks=1` was used; with `acks=all` this situation cannot arise).

## Unclean Leader Election

When all ISR members for a partition are simultaneously unavailable, the partition has no eligible leader. Two choices:

**Disabled (default — `unclean.leader.election.enable=false`):** the partition remains offline. Producers receive `LeaderNotAvailableException`. Consumers cannot read. This state persists until at least one ISR member recovers and is elected leader. No data is lost — the committed log is intact on the recovering broker.

**Enabled:** an out-of-sync replica is allowed to become leader. The partition comes back online immediately. However, the new leader's log is behind the old leader's committed log — any records that were committed to the old ISR but not yet replicated to this follower are permanently gone. Consumers that already read those records will not see them again; downstream systems may have already acted on them.

**Decision rule:** disable unclean election for any topic where data loss is unacceptable. Enable only for topics where availability is more important than completeness — operational metrics, low-fidelity logs, or topics where downstream consumers are idempotent and can tolerate gaps.

## Rack-Aware Replica Placement

Configure `broker.rack` on each broker to identify its physical location:

```properties
# On brokers in each AZ
broker.rack=us-east-1a   # broker 1
broker.rack=us-east-1b   # broker 2
broker.rack=us-east-1c   # broker 3
```

With rack awareness enabled, Kafka's partition assignment algorithm ensures that replicas for a given partition are placed on brokers in different racks before reusing any rack. For RF=3 across 3 AZs, each replica lands in a distinct AZ — a complete AZ failure takes down at most one replica per partition, preserving the ISR.

Without rack awareness, the assignment algorithm distributes replicas round-robin across brokers by broker ID. In a 6-broker cluster with 2 brokers per AZ, a partition's three replicas may end up on brokers 1, 2, and 3 — all in the same AZ.

Rack-aware assignment only applies at topic creation time. Existing topics require partition reassignment to correct non-rack-aware placements.

## Alerting on Replication Health

| Metric | Alert level | Meaning |
|---|---|---|
| `UnderReplicatedPartitions > 0` | Warning → Incident if sustained | One or more followers have fallen out of ISR; RF is degraded |
| `OfflinePartitionsCount > 0` | Critical | Partition has no available leader; producers and consumers are blocked |
| `IsrShrinkRate` sustained spike | Warning | Systematic follower lag — investigate broker resource saturation or network |
| `IsrExpandRate` lag behind `IsrShrinkRate` | Warning | Replicas falling out faster than they recover — cluster stability declining |

Under-replicated partitions are not immediately critical — the cluster continues operating — but they indicate the cluster is one more broker failure away from offline partitions or data loss. Treat any sustained non-zero value as an incident.
