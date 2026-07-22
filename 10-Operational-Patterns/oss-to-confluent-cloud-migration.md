# OSS Kafka to Confluent Cloud Migration

Migrating from a self-managed OSS Kafka cluster to Confluent Cloud is a sequence of coordinated phases, not a single cutover event. The primary tools — Cluster Linking, Schema Linking, and the `promote` command — allow zero-downtime migration with exact offset parity. The key risk is doing the phases out of order. This phased, both-systems-running-in-parallel approach is a Strangler Fig migration: traffic moves incrementally while the old and new systems coexist, rather than a single big-bang cutover.

---

## Pre-Migration Assessment

### CKU Sizing

Confluent Cloud capacity is expressed in elastic CKUs (eCKUs), not broker count. The Kora engine handles scaling and partition rebalancing automatically — you declare throughput targets, not instance sizes.

Sizing from OSS metrics:

| OSS metric | CC implication |
|---|---|
| Peak produce MB/s across all topics | Primary eCKU sizing input |
| Peak consume MB/s (all consumer groups combined) | Consumer throughput is metered separately |
| Partition count | No direct CKU impact; CC handles rebalancing |
| Replication factor | Not a cost lever — CC manages replication internally |

Rule of thumb: 1 eCKU handles ~50 MB/s produce + 150 MB/s consume. Start with observed peak * 1.5 headroom; autoscaling handles spikes beyond that.

This elastic, autoscaling behavior applies to Basic/Standard tiers. If your migration target is Dedicated — required for PrivateLink or broker-side schema validation, see Phase 1 below — CKU sizing is fixed, not autoscaling: `01-Core-Concepts/kafka-vs-confluent.md`. Sustained growth on Dedicated is a manual resize, not something the platform absorbs; for a repeatable resize cadence under compounding growth, see `13-Performance-Tuning/capacity-scaling-cadence.md`.

### Feature Gap Assessment

Verify before planning:

| OSS capability | CC equivalent | Gap |
|---|---|---|
| ZooKeeper-based metadata | KRaft (internal) | No migration needed — CC is already KRaft |
| Custom broker plugins | Not supported | Assess each plugin; most are replaced by CC features |
| Self-hosted Schema Registry | Confluent Cloud SR | Schema Linking handles sync |
| Self-managed Kafka Connect | Managed connectors (80+) | Audit which connectors have managed equivalents |
| Custom `CreateTopicPolicy` | OPA + GitOps | See [opa-policy-enforcement.md](opa-policy-enforcement.md) |
| SASL/PLAIN or SCRAM auth | OAuth/OIDC or API keys | Dual-listener transition required |
| JMX metrics | Confluent Metrics API | See [11-Monitoring-Observability/confluent-cloud-metrics-api.md](../11-Monitoring-Observability/confluent-cloud-metrics-api.md) |

---

## Migration Phases — Ordered Sequence

### Phase 1 — Network

Establish connectivity before any data moves. This is the hard dependency everything else blocks on.

**Options:**
- **VPC peering / transit gateway** — lower latency, preferred for high-throughput migrations
- **PrivateLink** — required if your security policy prohibits public endpoints; available on Dedicated clusters
- **Public internet + TLS** — viable for non-sensitive migrations; no setup overhead

Set up the network path and verify TCP connectivity on port 9092 (Kafka) and port 443 (Schema Registry, REST) before proceeding. See [09-Security-Architecture/private-networking.md](../09-Security-Architecture/private-networking.md) for PrivateLink configuration.

### Phase 2 — Schema Linking

Schema Linking keeps schemas continuously synchronized from your self-hosted Schema Registry to Confluent Cloud SR. Start it early — schemas must be present before Cluster Linking starts replicating topic data, or consumers on CC will fail to deserialize.

```bash
# Create a schema exporter from self-hosted SR to CC SR
confluent schema-registry exporter create oss-to-cloud-exporter \
  --subjects "*" \
  --config sr-exporter.properties
```

`sr-exporter.properties`:
```properties
schema.registry.url=https://your-oss-sr:8081
confluent.schema.registry.url=https://<cc-sr-endpoint>
confluent.schema.registry.basic.auth.user.info=<key>:<secret>
```

Schema Linking is **continuous**, not a one-time export. It stays live throughout the migration window, picking up any new schemas registered on the source. It supports active-passive (source is authoritative) and can be left running until you're ready to decommission the source SR.

**After the link is established, verify:** all subjects from the source appear in CC SR with identical schema IDs. Schema ID parity is required for consumers to decode records from mirror topics without reconfiguration.

### Phase 3 — Cluster Link Setup

Cluster Linking uses the native Kafka replica fetcher protocol extended across cluster boundaries. Mirror topics are byte-for-byte replicas — identical partition count, identical offsets, identical topic configs.

```bash
# Create the cluster link (destination-initiated — CC pulls from OSS source)
confluent kafka link create oss-to-cloud-link \
  --cluster <cc-cluster-id> \
  --config cluster-link.properties

# cluster-link.properties
bootstrap.servers=<oss-broker-1>:9092,<oss-broker-2>:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=...
consumer.offset.sync.enable=true
consumer.offset.sync.ms=5000
acl.sync.enable=true
```

**Firewall-constrained environments:** If your OSS cluster is behind a strict egress firewall that blocks outbound connections to CC, use a **source-initiated link** instead — the OSS cluster pushes to CC rather than CC pulling from OSS. This requires Confluent Platform 7.x or the source-initiated link plugin on OSS Kafka.

**Enable auto-create mirror topics** to avoid manually mirroring each topic:

```bash
confluent kafka link update oss-to-cloud-link \
  --config auto.create.mirror.topics.enable=true
```

With `auto.create.mirror.topics.enable=true`, every topic on the source is mirrored automatically. Mirror topics on CC are read-only (`ACTIVE` state) while the link is live.

**What syncs automatically via the link:**
- Topic data (byte-for-byte, partition-for-partition)
- Consumer group offsets (`consumer.offset.sync.enable=true`)
- ACLs (`acl.sync.enable=true`)
- Topic configuration changes (retention, cleanup policy, etc.)

### Phase 4 — Auth Transition

Auth migration is independent of data migration. Run the two in parallel — there is no dependency between Cluster Linking and auth.

The safest path is a dual-listener window:

1. Keep the existing OSS auth listener active (SASL/SCRAM or SASL/PLAIN)
2. Add OAuth/OIDC as the CC auth mechanism
3. Migrate producers and consumers to OAuth in waves — no forced cutover
4. Decommission OSS auth only after all clients are on OAuth

For the CC side, configure OAuth service principals or API keys depending on whether your clients support OIDC token refresh. See [09-Security-Architecture/mtls-oauth.md](../09-Security-Architecture/mtls-oauth.md) and [09-Security-Architecture/cel-identity-pools.md](../09-Security-Architecture/cel-identity-pools.md) for Identity Pool setup.

### Phase 5 — Data Promotion (Planned Cutover)

When you are ready to cut over, use `promote` — not `failover`. `promote` is the planned migration command. It performs a **final synchronization** of topic configs and consumer offsets before transitioning each mirror topic from `ACTIVE` (read-only) to `STOPPED` (writable).

```bash
# Promote a single topic
confluent kafka mirror promote <topic-name> --link oss-to-cloud-link

# Promote all topics on the link at once
confluent kafka mirror promote --link oss-to-cloud-link --all-topics
```

What `promote` does:
1. Waits for the mirror to fully catch up to the source's log end offset (LEO)
2. Performs one final sync of topic config and consumer offsets
3. Transitions the mirror topic state from `ACTIVE` → `STOPPED`
4. The topic is now writable on CC and independent of the source

After promotion, the mirror topic is no longer linked — writes go directly to CC.

**`failover` vs `promote`:**

| Command | Use case | Data loss risk |
|---|---|---|
| `promote` | Planned migration — source available | Zero — waits for full LEO catch-up |
| `failover` | Disaster — source unavailable | Possible — clamps offsets to current LEO |

Never use `failover` for a planned migration.

### Phase 6 — Client Cutover

Because Cluster Linking preserves exact offsets, consumers on CC pick up exactly where the OSS consumers left off — no offset translation, no replay.

#### Cutover Order: Consumers Before Producers

Always move consumers to CC before producers. The order is forced by the Cluster Linking state machine:

- While the link is live, CC mirror topics are **read-only** (`ACTIVE` state) — producers cannot write to CC yet
- `promote` runs only after consumers are stable on CC; it makes CC topics writable
- If you move producers first, OSS stops receiving writes, consumers left on OSS drain to the end of the log, and — with no reverse link from CC to OSS — they never see new records

```
1. Consumers → CC     (reading mirror topics; OSS still feeds via Cluster Linking)
2. Confirm all consumer groups stable on CC, lag = 0
3. promote            (final sync; CC topics become writable)
4. Producers → CC     (writing directly to CC)
5. Decommission OSS
```

#### Pure Consumer Services

1. Confirm consumer lag on OSS = 0 for this service's consumer group
2. Update `bootstrap.servers` to CC endpoint; update auth config
3. Restart — resumes from the synced offset, no replay

#### Pure Producer Services

Move after `promote` has completed. Switch `bootstrap.servers` and credentials. Idempotent producers (`enable.idempotence=true`) handle any in-flight duplicates during the switchover window.

#### Services That Are Both Producer and Consumer

Cannot be split — cutover must be atomic. Attempting a partial switch (consumer to CC, producer still on OSS) risks split-brain if the service reads its own output topic, and creates unnecessary Cluster Linking round-trips for everything else.

**Sequence:**
1. Confirm consumer lag = 0 for this service's consumer group on OSS
2. **Stop the service** — brief controlled shutdown (seconds to low minutes)
3. Run `promote` for all topics this service produces to (if not already promoted)
4. Update `bootstrap.servers` and auth for both producer and consumer config
5. Restart — resumes consuming from the synced offset and produces directly to CC

The brief stop is the price of safety. Consumer offsets are synced to CC before promote, so the service resumes exactly where it left off.

**Kafka Streams applications** are the canonical dual-role case — the application ID governs both input consumption and internal changelog/repartition topic production. There is no way to split Kafka Streams producer from consumer; always use the stop → promote → restart sequence.

If changelog topics were replicated via Cluster Linking, the state store resumes without a full rebuild. If they were not replicated, or partition counts differ, the state store rebuilds from scratch — for large RocksDB stores, use S3 pre-seeding to avoid hour-long cold starts. See [rocksdb-s3-preseeding.md](rocksdb-s3-preseeding.md).

### Phase 7 — Connect Migration

Audit each self-managed connector against the [Confluent Hub](https://www.confluent.io/hub/) managed connector catalog (80+ connectors). Three outcomes:

| Scenario | Action |
|---|---|
| Managed equivalent exists | Recreate in CC using the Confluent Cloud connector API; migrate SMT chain config |
| No managed equivalent | Deploy self-managed Connect worker connecting to CC cluster |
| Custom connector | Deploy self-managed Connect worker; not eligible for managed hosting |

For connectors with managed equivalents, use the CC connector migration APIs to import existing connector configurations:

```bash
confluent connect cluster import \
  --cluster <cc-cluster-id> \
  --config connector-export.json
```

Self-managed Connect workers targeting CC are configured identically to any Kafka client — set `bootstrap.servers` to the CC endpoint and use API key or OAuth credentials.

### Phase 8 — Decommission

Decommission in reverse dependency order:

1. Confirm zero consumer lag on CC for all consumer groups
2. Confirm all producers are writing to CC (monitor OSS produce rate → 0)
3. Delete the Cluster Link (`confluent kafka link delete oss-to-cloud-link`)
4. Shut down self-managed Connect workers that have been replaced by managed connectors
5. Decommission self-hosted Schema Registry (Schema Linking can be stopped once SR is no longer the source of truth)
6. Shut down OSS brokers
7. Decommission ZooKeeper ensemble (if still present — KRaft-mode OSS clusters skip this)

Do not delete the Cluster Link until all producers have switched to CC. Deleting the link while OSS is still receiving writes means those writes will never reach CC.

---

## Connectivity Pattern Decision

```
Does your OSS cluster have outbound internet access to Confluent Cloud?
├── Yes → Destination-initiated link (CC pulls from OSS) — simpler, default
└── No (strict egress firewall)
    ├── Can you open egress to CC IPs? → Add firewall rules, use destination-initiated
    └── Cannot open egress → Source-initiated link (OSS pushes to CC)
                             Requires CP 7.x or source-initiated link plugin
```

---

## What Does Not Migrate Automatically

| Item | Manual action required |
|---|---|
| Consumer group offsets for groups with zero lag | Already synced — nothing to do |
| Consumer group offsets for groups with active lag | Sync happens continuously; promote waits for catch-up |
| Schema Registry schemas | Schema Linking handles this, but verify schema ID parity |
| KRaft controller metadata | Not migrated — CC uses its own internal KRaft |
| Broker-side topic policies (`CreateTopicPolicy`) | Replace with OPA conftest policies in CI |
| Custom authorizer implementations | Replace with CC RBAC + Identity Pools |
| Prometheus/JMX dashboards | Repoint to Confluent Metrics API; metrics names differ |
| ZooKeeper-based tooling (e.g., Cruise Control) | CC has built-in equivalents; Cruise Control not applicable |

---

## Cross-References

- Cluster Linking internals and offset parity — [12-Multi-Region-DR/cluster-linking.md](../12-Multi-Region-DR/cluster-linking.md)
- Topic cutover state machine (DUAL_WRITE → CUTOVER) — [10-Operational-Patterns/blue-green-topic-migration.md](blue-green-topic-migration.md)
- Auth migration (SASL → OAuth dual-listener) — [09-Security-Architecture/multi-protocol-auth.md](../09-Security-Architecture/multi-protocol-auth.md)
- Identity Pools and OAuth on CC — [09-Security-Architecture/cel-identity-pools.md](../09-Security-Architecture/cel-identity-pools.md)
- PrivateLink and VPC networking — [09-Security-Architecture/private-networking.md](../09-Security-Architecture/private-networking.md)
- RocksDB S3 pre-seeding for Kafka Streams cold start — [10-Operational-Patterns/rocksdb-s3-preseeding.md](rocksdb-s3-preseeding.md)
- OPA policy enforcement (replacing `CreateTopicPolicy`) — [10-Operational-Patterns/opa-policy-enforcement.md](opa-policy-enforcement.md)
- Confluent Cloud Metrics API (replacing JMX) — [11-Monitoring-Observability/confluent-cloud-metrics-api.md](../11-Monitoring-Observability/confluent-cloud-metrics-api.md)
