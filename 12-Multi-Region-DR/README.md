# Module 12 — Multi-Region and Disaster Recovery

Replication topologies, technology tradeoffs, and RPO/RTO characteristics for multi-region Kafka deployments.

## Files

- [cluster-linking.md](cluster-linking.md) — How Cluster Linking works as a native protocol extension, offset parity, consumer group failover mechanics, and when to choose it over MirrorMaker 2.
- [mirrormaker2.md](mirrormaker2.md) — MirrorMaker 2 architecture as a Kafka Connect framework, offset translation, consumer group migration with timestamp interceptors, and operational tradeoffs.
- [active-active-dr.md](active-active-dr.md) — Multi-region topology patterns (active-passive, active-active, hub-and-spoke, stretched cluster), RPO/RTO tradeoff table, and decision heuristics for technology and topology selection.
