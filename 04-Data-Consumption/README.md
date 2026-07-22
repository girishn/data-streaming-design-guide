# Module 04 — Data Consumption

Consumer group mechanics, partition assignment strategies, and techniques for minimising rebalance disruption in production and Kubernetes deployments.

## Files

- [consumer-groups.md](consumer-groups.md) — Group Coordinator, state machine, partition assignors, offset commit strategies, and the JoinGroup/SyncGroup/Heartbeat protocol flow.
- [cooperative-rebalancing.md](cooperative-rebalancing.md) — Stop-the-world eager rebalancing vs incremental cooperative rebalancing, CooperativeStickyAssignor, migration path, and limitations.
- [static-membership.md](static-membership.md) — Rolling restart rebalance storms, group.instance.id identity, the failure-detection trade-off, and StatefulSet configuration for Kubernetes.
- [retry-topic-pattern.md](retry-topic-pattern.md) — Staged backoff via a chain of retry topics instead of in-process retry: topic-per-stage design, header-based retry tracking, tradeoffs against in-process retry, and when the added complexity is worth it.
