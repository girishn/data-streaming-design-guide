# Cooperative Rebalancing — Incremental Partition Handoff

## The Stop-the-World Problem

Classic Kafka rebalancing uses an **eager protocol**: before any new assignment can be computed, every consumer in the group must revoke all of its partitions. The sequence is:

1. Coordinator signals all members to rejoin (PreparingRebalance)
2. All consumers stop processing and revoke every partition they hold
3. All members send JoinGroup
4. Leader computes new assignment, sends SyncGroup
5. All members receive new assignments and resume

During steps 2–5, no consumer in the group is processing any partition. For a group with hundreds of partitions and consumers running stateful applications backed by RocksDB, closing and flushing state stores on revocation takes time. A rebalance triggered by adding a single consumer can halt the entire group for 30 seconds or more — and in Kubernetes environments with frequent rolling restarts, this happens continuously.

The pause duration scales with:
- Number of partitions being revoked (proportional to group size × topic partition count)
- Time to flush and close local state stores
- Network round-trip time for JoinGroup/SyncGroup RPCs

## Cooperative Incremental Rebalancing

Introduced in Kafka 2.4, the **cooperative incremental rebalance protocol** eliminates the global pause by splitting the rebalance into two targeted phases:

**Phase 1 — Revocation only:**
The group leader identifies which partitions must change hands (the minimal set required to achieve balance). Only the current owners of those specific partitions are asked to revoke them. Every other consumer continues processing its retained partitions without interruption.

**Phase 2 — Assignment:**
Once the revoked partitions are available, a second, lightweight rebalance assigns them to their new owners. Consumers that retained their partitions do not participate in this second round beyond acknowledging it.

The result: during a scale-out or rolling restart, only the partitions that are actually moving experience processing downtime. A group consuming 100 partitions where 5 need to move experiences a pause on those 5 partitions — not all 100.

## CooperativeStickyAssignor

The **`CooperativeStickyAssignor`** is the recommended assignor for all new deployments. It combines two properties:

- **Sticky:** preserves existing partition-to-consumer mappings wherever possible, minimising the number of partitions that need to move
- **Cooperative:** uses the two-phase incremental protocol so unmoved partitions never stop processing

Configure it explicitly:

```properties
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

In Kafka 3.1+, the default was changed to `[RangeAssignor, CooperativeStickyAssignor]` for backward compatibility during rolling upgrades. Once all consumers in a group are on 3.1+, setting `CooperativeStickyAssignor` alone is safe and preferred.

## Migrating from Eager Assignors

A group cannot mix eager and cooperative consumers — a single eager consumer in a cooperative group forces the entire group to fall back to eager behaviour. Migration requires a two-step rolling upgrade:

**Step 1:** Update all consumers to use both assignors in order:
```properties
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor,org.apache.kafka.clients.consumer.RangeAssignor
```
Roll this out across all instances. During this window, the group negotiates the highest protocol both sides support.

**Step 2:** Once all instances are running Step 1 config, remove `RangeAssignor`:
```properties
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Skipping Step 1 and jumping directly to cooperative-only on a subset of consumers will trigger eager rebalances on every deployment until the rollout completes.

## When Cooperative Rebalancing Does Not Help

Cooperative rebalancing reduces disruption only when the underlying assignment is stable — i.e., most partitions stay with their current owners and only a few move.

**RoundRobinAssignor + cooperative protocol:** on any group membership change, RoundRobin rotates the entire partition list across members. Nearly every partition ends up with a different owner. The cooperative protocol still executes two phases, but Phase 1 revokes almost all partitions and Phase 2 reassigns almost all of them. The consumer group still experiences near-total processing downtime, plus the additional complexity of two rebalance rounds instead of one.

**Large group churn:** if consumers are crashing and rejoining frequently (due to unhealthy pods, long GC pauses, or processing timeouts), cooperative rebalancing reduces per-rebalance impact but does not reduce rebalance frequency. Address the root cause — see `04-Data-Consumption/static-membership.md` for planned restarts, and tune `max.poll.interval.ms` and `max.poll.records` for processing timeout issues.
