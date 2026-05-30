# Blue-Green Topic Migration

Kafka topic partition count is immutable after creation. The `murmur2(key) mod N` mapping changes when N changes — a key that hashed to partition 7 of 12 hashes to a different partition with 48. Changing partition count in-place breaks per-key ordering for any consumer that relies on it (state machines, ledgers, loyalty calculations). The blue-green migration pattern is the safe alternative: create a parallel green topic, mirror or dual-write data into it, drain consumers from blue to green, then decommission blue.

The same pattern applies when you need to change cleanup policy (compacted → delete), move to a new topic naming convention, or change the key schema in a way that requires a new `TopicNameStrategy` subject.

See `02-Broker-Infrastructure/partitioning-strategies.md` for the short-form version. This file covers the full operational procedure including platform team responsibilities.

---

## Migration State Machine

Track migration state explicitly. Every team involved (platform and app) should see the same state.

```
CREATED → DUAL_WRITE → DRAINING → CUTOVER → DECOMMISSIONED
```

Persist state in your control plane (a Kafka topic tracking migrations, a config map, or an internal service). Never allow implicit state — if you don't know what phase you're in, you can't safely roll back.

---

## Step 1 — Provision the Green Topic

Create the green topic with the target partition count. Config parity with blue is a hard requirement — divergence causes silent behavior differences post-cutover.

Checklist:
- `replication.factor` matches blue
- `min.insync.replicas` matches blue
- `retention.ms` and `retention.bytes` match blue
- `cleanup.policy` matches blue (or is the intentional change)
- `compression.type` matches blue
- ACLs cloned from blue (or deliberately revised)

Use Terraform (Confluent provider) to enforce parity — a manual `kafka-topics.sh --create` will drift. The green topic configuration should be a copy of the blue topic resource with only the intended parameters changed.

Register schemas in Schema Registry under the green subject name. If using `TopicNameStrategy`, the green topic requires a new subject (e.g., `payments-v2-value`). Set compatibility to `BACKWARD` so consumers can read both old and new schemas during the transition window.

Transition state: **CREATED**

---

## Step 2 — Initiate Dual-Write or Replication

Two approaches. Pick based on whether the application team or the platform team should carry the transition risk.

### Option A — Platform-Managed Replication (preferred)

Use **MirrorMaker 2** or **Cluster Linking** to mirror blue → green asynchronously. The application team makes no producer changes during the transition.

**Cluster Linking** (Confluent Platform/Cloud): creates a byte-for-byte replica. Offsets on the green topic are identical to the corresponding offsets on the blue topic. Consumer offset sync is native — no translation step needed. This is the lowest-risk path when both clusters (or a source and mirror topic on the same cluster) support Cluster Linking.

**MirrorMaker 2**: framework-based replication. Offsets on the green topic are not identical to blue — MM2 uses `ConsumerTimestampsInterceptor` to commit timestamps on the source and then maps those timestamps to green offsets. An explicit offset translation step is required before consumers cut over. Adds operational complexity but works across clusters that don't support Cluster Linking.

Green topic will lag blue by the replication latency (typically seconds). This is acceptable — consumers won't cut over until blue lag reaches zero.

### Option B — Application-Side Dual-Write

Configure producers to write to both blue and green simultaneously. The producer client sends each event to both topics in the same call (two `send()` calls per event, not a transaction across topics unless you use exactly-once semantics).

Doubles write amplification during the transition window — verify the cluster has headroom. An idempotent producer (`enable.idempotence=true`) prevents retry-induced duplicates on each topic, but events may still arrive on green in slightly different order than blue if the two sends interleave with retries. Downstream consumers must be idempotent.

Prefer Option A when possible. Option B puts migration complexity on every producer team.

Transition state: **DUAL_WRITE**

---

## Step 3 — Drain Blue Consumers

Before cutting consumers over, drain the blue topic to zero lag. This ensures no events are lost during cutover.

Monitor consumer lag on both topics in parallel during the dual-write window. Alert on:
- Lag growth on the green topic (replication falling behind, or a misconfigured consumer starting on green before drain completes)
- Lag on blue stalling (consumer stopped processing before drain)
- Producer error rate spike on green (schema compatibility failure, ACL missing)

Once blue consumer lag for all consumer groups reaches zero, the drain is complete.

Transition state: **DRAINING** → ready for cutover once lag = 0 on blue for all groups

---

## Step 4 — Offset Translation and Consumer Cutover

The critical step. Without offset translation, consumers starting on green replay from offset 0 — processing the entire topic history again.

**With Cluster Linking:** offset parity is exact. Consumers can be pointed at green without translation. Use the consumer group offset sync feature to copy committed offsets from blue to green before cutover.

**With MirrorMaker 2:** use MM2's offset translation consumer (`kafka-consumer-groups.sh` with the `RemoteClusterUtils` translation map, or MM2's own `RemoteTopicNameTranslator`). Platform teams should expose this as a self-service CLI:

```
platform migrate-offsets \
  --from payments \
  --to payments-v2 \
  --groups payment-processor,payment-auditor
```

Run offset translation as a dry-run first to verify the mapping before consumers are reconfigured.

**With producer-side dual-write:** the green topic has the same events but potentially different offsets. Offset translation is still required. If you've been dual-writing with idempotent producers, the sequence of events is equivalent but not offset-equivalent.

**Consumer rebalance:** redirect consumer group `bootstrap.servers` and `topic` config to the green topic, then trigger a controlled rebalance. Use `CooperativeStickyAssignor` — it performs incremental partition assignment and avoids the stop-the-world pause that `RangeAssignor` causes. A stop-the-world rebalance at cutover spikes end-to-end latency for all consumers in the group simultaneously.

**Stateful Kafka Streams applications:** switching topics forces a state store rebuild from the new topic's changelog. For large state stores (RocksDB with hundreds of millions of keys), this can take hours. Use RocksDB S3 pre-seeding to download a state snapshot rather than replaying the full topic history — see `10-Operational-Patterns/rocksdb-s3-preseeding.md`.

Transition state: **CUTOVER**

---

## Step 5 — Decommission Blue

Once all consumer groups are confirmed healthy on green:

1. Stop dual-production (or stop the replication link)
2. Verify blue consumer lag stays at zero (no straggler consumers)
3. Deprecate the blue schema subject in Schema Registry — set compatibility to `NONE` and document as deprecated, or delete if no consumers can reach it
4. Delete the blue topic after a retention buffer (retain for at least one full retention period of the green topic to allow rollback if needed)
5. Remove blue from monitoring dashboards and alert configs

Transition state: **DECOMMISSIONED**

---

## Platform Support Responsibilities

The migration procedure spans both the platform team (infrastructure, tooling) and the application team (producer/consumer config). The platform team owns the following:

**Provisioning tooling**
Terraform module accepting `(blue_topic, green_topic, partition_count, config_overrides)` that creates the green topic and validates config parity against blue before applying. Expose a config-diff check as a blocking pre-apply validation.

**Schema Registry orchestration**
Script to clone the blue subject's schema to the green subject with the correct compatibility setting. Platform team should not require app teams to manually re-register schemas.

**Replication setup**
Platform-managed MM2 or Cluster Linking infrastructure. App teams should not configure replication themselves — they request a migration and the platform provisions the link.

**Offset translation CLI**
`platform migrate-offsets` self-service command with dry-run support. Platform owns the underlying MM2 offset map or Cluster Linking offset sync — app teams provide consumer group names.

**Migration state tracking**
A control plane that tracks the `CREATED → DECOMMISSIONED` state machine, visible to both platform and app teams. State transitions should be auditable (who moved it, when).

**Observability during migration**
Side-by-side consumer lag dashboard for blue and green. Separate alert policies for each topic during the window — do not merge blue and green lag into a single metric. Alert owners should be aware migration is in progress to avoid false-positive escalations.

**Rollback gate**
Define a rollback SLA: if green consumer error rate exceeds a threshold within a window after cutover, auto-revert consumer group config back to blue. This requires blue to still be producing (or the replication link still active) at the time of rollback.

---

## Rollback

The blue-green model is inherently rollback-safe as long as the blue topic remains active and producing during the transition.

**During DUAL_WRITE / DRAINING:** rollback is stopping dual-write and not advancing to CUTOVER. Blue was never stopped. Cost: zero.

**During CUTOVER:** if consumer error rate spikes, redirect consumer groups back to blue. Blue committed offsets are still valid (consumers were drained to blue lag = 0 before cutover). Re-establish dual-write if producers have already been redirected to green.

**After CUTOVER, before DECOMMISSIONED:** keep blue alive for the full rollback window. Use KIP-875 admin REST APIs (`/v3/clusters/{id}/consumer-groups/{id}/offsets`) to reset consumer groups to blue offsets if a logic error is detected on green that requires full replay.

**After DECOMMISSIONED:** no rollback path. Decommission only after the rollback window has passed and all consumers are confirmed stable.

---

## Decision: Cluster Linking vs MirrorMaker 2 vs Dual-Write

| Factor | Cluster Linking | MirrorMaker 2 | Dual-Write |
|--------|----------------|---------------|------------|
| Offset parity | Exact | Requires translation | Requires translation |
| Setup ownership | Platform team | Platform team | App team |
| Write amplification | None (replication) | None (replication) | 2× on producers |
| Cross-cluster support | Same or linked clusters | Any clusters | Any |
| Consumer offset sync | Native | Manual translation step | Manual translation step |
| Availability | Confluent Platform/Cloud | OSS + Confluent | Any |

Use Cluster Linking when available. It eliminates the offset translation problem entirely and reduces migration complexity to a consumer config change.
