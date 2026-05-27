# Multi-Region Topology Patterns — RPO, RTO, and Decision Factors

## The Core Trade-off

Multi-region Kafka deployments exist on a spectrum between two extremes: synchronous replication (zero data loss, high latency cost) and asynchronous replication (low latency cost, non-zero data loss window). Every topology choice is a position on this spectrum, and the right position depends on the business's actual RPO and RTO requirements — not the aspirational ones.

**RPO (Recovery Point Objective):** the maximum acceptable amount of data loss measured in time. RPO = 0 means no data loss is acceptable. RPO = 5 minutes means losing up to 5 minutes of data is acceptable in a disaster.

**RTO (Recovery Time Objective):** the maximum acceptable time to restore service after a failure. RTO = 30 seconds means the service must be processing again within 30 seconds of a failure.

## Topology Patterns

### Active-Passive (Primary + Standby)

One cluster (primary) handles all reads and writes. Data is asynchronously replicated to a standby cluster. The standby is not serving traffic under normal conditions.

```
[Producers] → [Primary Cluster] → (async replication) → [Standby Cluster]
[Consumers] → [Primary Cluster]

On failure:
[Producers] → [Standby Cluster] (after client reconfiguration)
[Consumers] → [Standby Cluster]
```

**RPO:** Determined by replication lag — the volume of data produced to the primary that has not yet been replicated to the standby at the time of failure. With Cluster Linking at low lag, this is seconds. With MirrorMaker 2 under load, this can be tens of seconds to minutes.

**RTO:** Time to detect the failure, trigger failover (promote/failover command or automated), and reconfigure clients to the standby. With Cluster Linking's `failover` command and pre-built automation, this is seconds to low minutes.

**Use when:** Regulatory requirements for a geographically separate copy (audit, compliance). Simple DR without the complexity of bidirectional replication. Read replicas for analytical consumers that do not need to write.

### Active-Active (Bidirectional)

Both clusters handle reads and writes. Each cluster replicates to the other. All records produced in either cluster are eventually visible in both.

```
[Region A Producers] → [Cluster A] ←→ (bidirectional replication) ←→ [Cluster B] ← [Region B Producers]
[Region A Consumers] → [Cluster A]                                      [Cluster B] → [Region B Consumers]
```

**With MirrorMaker 2:** MM2's default `DefaultReplicationPolicy` prefixes replicated topics with the source cluster alias (`us-east.orders` on the destination), preventing replication loops. Records written to `orders` in `us-east` appear as `us-east.orders` in `eu-west`, and vice versa. Consumers must subscribe to both `orders` and `{remote-alias}.orders` to see the full global stream.

**With Cluster Linking:** Bidirectional Cluster Links can be configured — one link from A to B, one from B to A. Without topic name isolation, loop prevention requires careful topic inclusion/exclusion configuration.

**Conflict resolution:** Active-active does not have built-in conflict resolution at the Kafka layer — Kafka topics are append-only logs. Conflicts (two producers writing conflicting updates to the same entity key in different regions simultaneously) are a consumer-side or application-side concern. Design approaches:
- **Last-write-wins by timestamp:** consumers use the record timestamp to determine which version to apply. Requires synchronized clocks (NTP, PTP) across regions.
- **CRDT-based state:** application state is designed to be conflict-free by construction (counters, sets with tombstones).
- **Geo-routing:** route all writes for a given entity to a designated home region. No conflicts because the entity has a single writer. Routes writes to the correct region at the load balancer or application layer.

**RPO:** Near-zero for records produced to the local cluster (those records are always available locally). For records produced to the remote cluster, RPO equals replication lag.

**RTO:** Near-zero — both clusters are already serving traffic. A regional failure removes one cluster; the other continues without reconfiguration.

**Use when:** Globally distributed producers and consumers where regional write latency matters. Fault tolerance without client reconfiguration on failure. Accepting the operational complexity of bidirectional replication and consumer-side conflict resolution.

### Hub-and-Spoke

**Fan-out (central → regional):** A central cluster replicates data outward to multiple regional clusters. Regional consumers read from their local cluster rather than the central cluster.

```
[Central Cluster] → [EU Cluster]
                 → [APAC Cluster]
                 → [US-WEST Cluster]
```

Use when data is produced centrally (batch ingestion, SaaS application events) but needs regional locality for consumers. Regional consumers get lower fetch latency by reading from a local cluster.

**Fan-in (regional → central):** Multiple regional or edge clusters push data inward to a central cluster for aggregation.

```
[Edge Cluster 1] → [Central Cluster]
[Edge Cluster 2] →
[Edge Cluster N] →
```

Common in IoT and industrial deployments: edge clusters are embedded in facilities, vehicles, or devices with limited connectivity. Each edge cluster buffers local events and replicates to a central cluster when connectivity is available. The central cluster aggregates all edge data for analytics, ML training, and global dashboards.

**RPO/RTO:** Depends on the replication technology used for each spoke (Cluster Linking or MM2) and the tolerance for edge cluster connectivity gaps.

### Stretched Cluster (Synchronous Replication)

A single logical Kafka cluster with brokers physically distributed across two or three availability zones or geographically close regions. Producers use `acks=all` with `min.insync.replicas=2` — a produce is only acknowledged after at least two replicas in different zones have written it. This is synchronous replication: the produce request blocks until replication is confirmed.

```
[Producers] → [Broker in Zone A] (acks=all)
                     ↓ (synchronous replication)
              [Broker in Zone B]
              [Broker in Zone C] (tiebreaker, if 3 zones)
```

**RPO:** Zero. A committed record exists on at least two independent sites before the client receives acknowledgment.

**RTO:** Milliseconds to seconds. The Kafka cluster continues operating as long as a quorum of brokers is available. No failover procedure — the cluster self-heals through leader election.

**Constraint:** Synchronous replication means producer latency includes the round-trip time between zones. At 1ms inter-zone RTT, this adds ~1ms to produce latency. At 10ms inter-zone RTT, this adds ~10ms. Geographic stretching beyond ~100km (where RTT exceeds ~5ms) typically makes `acks=all` latency unacceptable for high-throughput, low-latency workloads. Stretched clusters are an availability zone pattern (same region, multiple AZs), not a multi-region pattern.

## RPO/RTO Decision Table

| Topology | RPO | RTO | Requires Confluent? | Geographic Distance |
|---|---|---|---|---|
| Stretched cluster | Zero | Milliseconds | No | Same region, low-latency AZs |
| Active-passive + Cluster Linking | Seconds (replication lag) | Seconds (failover command) | Yes (both sides) | Any |
| Active-passive + MirrorMaker 2 | Minutes (lag + translation) | Minutes (offset translation) | No | Any |
| Active-active + Cluster Linking | Near-zero (local writes) | Near-zero (already serving) | Yes | Any |
| Active-active + MirrorMaker 2 | Near-zero (local writes) | Near-zero (already serving) | No | Any |
| Hub-and-spoke fan-in | Connectivity-dependent | Depends on spoke technology | Optional | Any |

## Decision Heuristics

**Zero RPO required AND regions are geographically close (same region, multi-AZ):** stretched cluster with `acks=all`. No replication technology needed — it is one cluster.

**Near-zero RPO required AND regions are far apart AND both clusters are Confluent:** active-passive or active-active with Cluster Linking. Offset parity eliminates consumer migration complexity at failover.

**Moderate RPO acceptable OR OSS Kafka on one or both sides:** MirrorMaker 2. Accept offset translation overhead at failover; build idempotent consumers.

**Global write distribution with regional latency requirements:** active-active with geo-routing to eliminate conflicts. Write to the region closest to the user; replicate to all others for read availability.

**IoT or edge data aggregation:** hub-and-spoke fan-in. Edge clusters tolerate connectivity gaps; central cluster is the aggregation point for analytics.
