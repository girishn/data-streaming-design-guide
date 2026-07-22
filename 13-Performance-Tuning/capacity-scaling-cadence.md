# Capacity Scaling Cadence — Designing for Repeated Growth

## The Problem This Solves

Most sizing guidance in this guide — partition-count formulas, CKU sizing, broker counts — answers "how do I size this correctly once." A platform growing 10x every 6 months needs a different answer: a repeatable cycle that fires every cycle without becoming a redesign each time. The two questions are related but distinct — get the one-time sizing wrong and you over- or under-provision; get the repeatable cadence wrong and every growth cycle becomes an emergency migration instead of a scheduled operation.

This file is the synthesis layer: it doesn't introduce new mechanisms, it sequences the ones that already exist elsewhere in the guide into a cycle, and states what triggers each step.

## Why a Fixed Multiplier Isn't Enough

`topic-design-framework.md` recommends a 2–3× growth multiplier on partition count at initial sizing. That absorbs one doubling-to-tripling of load. At a 10x/6-month rate, the multiplier is exhausted within the first cycle — by month 6 you need capacity the multiplier never anticipated, and by month 12 you're at 100x the original baseline. A one-time buffer cannot be sized large enough to cover compounding growth without absurd over-provisioning on day one (a 100x multiplier to survive a year is not a defensible initial sizing decision). The multiplier is correct for what it's for — protecting against near-term underestimation — but it is not a substitute for a repeatable resize cycle.

## The Three Things That Scale on Different Cadences

Compounding throughput growth doesn't force everything to resize at once. Three layers move independently:

| Layer | What triggers a resize | Mechanism | Disruption |
|---|---|---|---|
| **Broker / CKU capacity** | Sustained throughput or CPU utilization approaching ceiling | Add brokers (self-managed) or increase CKU count (Confluent Cloud Dedicated) | Low — capacity addition, no data movement required for existing topics |
| **Partition count** | Consumer parallelism ceiling reached, or per-partition throughput approaching hot-partition territory | `blue-green-topic-migration.md`'s dual-write/cutover procedure | High — requires the full migration state machine, not a live resize |
| **Quotas** | A single tenant/service's growth is outpacing its allocated share of cluster capacity | Adjust per-principal byte-rate/request-rate quotas (`quota-management.md`) | None — quota changes take effect immediately, no data movement |

Treat these as separate review cadences, not one combined "scaling day." Broker/CKU capacity and quota adjustments are cheap and should be reviewed on every growth-monitoring trigger (below). Partition increases are expensive and disruptive — batch them, and only trigger the blue-green procedure when consumer parallelism is the actual binding constraint, not preemptively.

## Monitoring Triggers — What Tells You a Cycle Is Starting

Don't schedule resizes on a calendar; trigger them off leading indicators, the same way `consumer-lag.md` recommends alerting on lag growth rate rather than absolute threshold:

- **Consumer lag growth rate** (`11-Monitoring-Observability/consumer-lag.md`) — sustained upward lag on a consumer group is the first sign that partition count or consumer instance count is becoming the binding constraint, ahead of any capacity metric telling you the same thing.
- **Broker CPU / request handler idle time** (self-managed) — `RequestHandlerAvgIdlePercent` trending down over weeks, not a single spike, signals broker-count exhaustion. See `13-Performance-Tuning/broker-tuning.md` for thread pool context.
- **eCKU-relative throughput headroom** (Confluent Cloud Basic/Standard) — these tiers autoscale within eCKU limits automatically; monitor produce/consume MB/s against the ~50 MB/s produce + 150 MB/s per-eCKU rule of thumb in `10-Operational-Patterns/oss-to-confluent-cloud-migration.md` to know when you're approaching a tier ceiling, not just a CKU count ceiling.
- **CKU utilization on Dedicated clusters** — Dedicated is fixed CKU sizing, not autoscaling (`01-Core-Concepts/kafka-vs-confluent.md`). Sustained utilization approaching capacity is a manual resize trigger, not something the platform absorbs on its own.
- **Per-principal quota saturation** (`13-Performance-Tuning/quota-management.md`) — a service consistently hitting its throttle is a signal that either its quota or the cluster's total capacity needs review, not that the client is misbehaving.

A platform scaling 10x every 6 months should review these signals at least monthly — a review cadence shorter than the growth cycle catches the trend before it becomes an incident, rather than discovering the ceiling the week it's hit.

## The Repeatable Cycle

1. **Monitor** the leading indicators above continuously; don't wait for a scheduled review to notice a trend.
2. **Classify the trigger**: is it a capacity ceiling (broker/CKU), a parallelism ceiling (partition count), or a tenant-specific imbalance (quota)? Different triggers go to different mechanisms — conflating them (e.g., adding partitions to fix a capacity problem, or adding CKUs to fix a consumer parallelism problem) wastes the cycle without resolving the actual constraint.
3. **Resize the cheap layers first**: broker/CKU count and quotas can typically be adjusted without disruption. Confirm this resolves the trigger before reaching for partition migration.
4. **Only if consumer parallelism is the binding constraint**, run the blue-green partition migration (`10-Operational-Patterns/blue-green-topic-migration.md`) as a planned, platform-owned operation — not an emergency response. Its Terraform module and offset-migration CLI exist specifically so this is a routine, repeatable action rather than a bespoke migration each time.
5. **Re-baseline the growth multiplier** (`topic-design-framework.md`) against the new throughput level, so the next cycle's early headroom is sized against current scale, not the original launch scale.
6. **Track cost trajectory alongside capacity** — compounding throughput growth compounds cost. Feed the same principal-scoped metrics used for showback (`11-Monitoring-Observability/confluent-cloud-metrics-api.md`) into the same review, so a resize decision and its cost impact are evaluated together, not discovered separately on the next bill.

## What Forces a Tier or Architecture Change Mid-Cycle

Two thresholds in this repeatable cycle are not just "resize and continue" — they're forced re-evaluations:

- **Basic/Standard → Dedicated.** Autoscaling within Basic/Standard eCKU limits stops being sufficient once sustained throughput or a compliance requirement (private networking, broker-side schema validation) crosses the threshold documented in `01-Core-Concepts/kafka-vs-confluent.md`. Once on Dedicated, capacity is fixed-CKU — the monitoring trigger for CKU utilization above becomes a manual, scheduled action instead of something the platform absorbs.
- **Self-managed cost crossover.** At the throughput and storage levels `01-Core-Concepts/kafka-vs-confluent.md`'s Cost Crossover section describes (10+ GB/s, 100TB+ storage), repeated 10x cycles eventually make self-managed infrastructure cheaper than continuing to scale CKUs — but only if the team has already built the operational muscle (blue-green migrations, quota discipline, monitoring) this cycle requires. A platform that has been running this cadence disciplined is in a much better position to make that move than one sizing for the first time under pressure.

## Two Axes That Scale Independently of Throughput

Not every growth dimension is driven by data volume. Two axes compound with *fleet size* and *schema count* instead, and neither is resolved by the broker/CKU/partition/quota cycle above. Both apply regardless of deployment model — Confluent Cloud does not abstract either one away.

**Client connection count / consumer fleet size.** A growing number of services connecting to the cluster can become the binding constraint before throughput or per-client quotas do. Confluent Cloud's own [service quota documentation](https://docs.confluent.io/cloud/current/quotas/service-quotas.html) states connection and request limits are "not strictly enforced" across Basic, Standard, Enterprise, and Dedicated tiers — meaning the platform does not cap this for you, and a compounding service count can still create connection storms and request-handler contention on Confluent Cloud, not just self-managed brokers. [KIP-612](https://cwiki.apache.org/confluence/display/KAFKA/KIP-612:+Ability+to+Limit+Connection+Creation+Rate+on+Brokers) exists precisely because connection creation storms can "stop brokers from doing other useful work... causing high request latencies or even unclean leader elections." Uber hit this from the consumer side: once internal consumer service count passed roughly 1,000, they built [uForwarder](https://www.uber.com/en/blog/introducing-ufowarder/), a push-based consumer proxy that decouples partition count from consumer service count — the proxy owns the Kafka connections and fans out to services over gRPC, so fleet growth no longer means proportional connection growth against the cluster. Monitor connection count per broker/cluster as its own trigger, separate from `num.network.threads` saturation (`13-Performance-Tuning/broker-tuning.md`); if fleet size rather than data volume is what's compounding, a consumer-proxy layer is the architectural fix, not more CKUs or partitions.

**Schema Registry subject/schema count.** As topic and event-type count grows, Schema Registry itself becomes a scaling concern — compatibility checking is the hot path and is CPU-bound, per Confluent's [Schema Registry best-practices guidance](https://www.confluent.io/blog/best-practices-for-confluent-schema-registry/), and a real production issue has been reported with request timeouts above roughly 4,000 schemas under concurrent load ([confluentinc/schema-registry#1277](https://github.com/confluentinc/schema-registry/issues/1277)). The mechanism differs by deployment model: self-managed Schema Registry hits this as an instance capacity/CPU problem, provisioned like any other service. Confluent Cloud instead gates schema/subject count by your Stream Governance package tier (Essentials vs. Advanced) — hitting the ceiling there is a package-upgrade decision, not a capacity-tuning one (`01-Core-Concepts/kafka-vs-confluent.md` for the Confluent Cloud tier model generally; `08-Stream-Governance/schema-evolution.md` for compatibility-mode and subject-naming choices that affect how fast subject count grows).

**Not covered here — self-managed only.** Controller memory pressure under sustained metadata churn and broker-decommission friction under continuous topic creation are real growth-driven risks reported at LinkedIn's scale, but they apply only to self-managed Kafka or Confluent Platform, where you operate the KRaft controller quorum and broker fleet yourself. Confluent Cloud's Kora engine absorbs both internally regardless of tier. See `02-Broker-Infrastructure/kraft-mode.md`.

## Cross-References

- One-time partition sizing formula and the growth-multiplier buffer this cadence re-baselines — [topic-design-framework.md](../topic-design-framework.md)
- The disruptive mechanism this cadence schedules rather than treats as an emergency — [10-Operational-Patterns/blue-green-topic-migration.md](../10-Operational-Patterns/blue-green-topic-migration.md)
- CKU sizing, eCKU autoscaling limits, and the Basic/Standard/Dedicated tier boundary — [10-Operational-Patterns/oss-to-confluent-cloud-migration.md](../10-Operational-Patterns/oss-to-confluent-cloud-migration.md), [01-Core-Concepts/kafka-vs-confluent.md](../01-Core-Concepts/kafka-vs-confluent.md)
- Lag growth-rate alerting as a leading indicator — [11-Monitoring-Observability/consumer-lag.md](../11-Monitoring-Observability/consumer-lag.md)
- Quota adjustment as the cheap, non-disruptive resize lever — [quota-management.md](quota-management.md)
- Cost tracking to run alongside every resize decision — [11-Monitoring-Observability/confluent-cloud-metrics-api.md](../11-Monitoring-Observability/confluent-cloud-metrics-api.md)
- Broker thread pool context for connection-count saturation — [13-Performance-Tuning/broker-tuning.md](broker-tuning.md)
- Schema Registry compatibility modes and subject-naming choices — [08-Stream-Governance/schema-evolution.md](../08-Stream-Governance/schema-evolution.md)
- Self-managed-only controller/broker-decommission growth risks — [02-Broker-Infrastructure/kraft-mode.md](../02-Broker-Infrastructure/kraft-mode.md)
