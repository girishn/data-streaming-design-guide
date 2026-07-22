# Streaming Design — Interview Cheat Sheet

Use this before any design problem or mock interview. Five minutes to reload the mental model.

---

## Step 1 — Discovery (ask before drawing anything)

Five question areas, in order:

| Area | Ask |
|---|---|
| **Sources** | Where does data live today? Cloud or on-prem? Event-driven or batch today? |
| **Sinks** | Who consumes downstream? How many independent teams? Raw events, aggregated state, or both? |
| **Latency / Scale** | What is the business target lag? Events per day — thousands, millions, billions? Cost of a 1-hour outage? |
| **Compliance** | PII in the data? GDPR, HIPAA, PCI? Data residency requirement? Right-to-erasure? |
| **Ownership** | First Kafka deployment or existing experience? Cloud-managed or self-managed preference? |

---

## Step 2 — Two Structural Gates (these two questions gate everything)

Answer these before any implementation decision. Getting them wrong forces rework at every later layer.

**Gate 1: Do consumers need ordering per entity?**
- Yes → single topic, entity ID as partition key. One-topic-per-event-type breaks this.

**Gate 2: Do consumers need to join across event types for the same entity?**
- Yes → single topic with `event_type` field. Separate topics destroy ordering across types.

**Gate 3: Do tenants need data isolation?**
- Hard (B2B / GDPR / ACLs) → per-tenant source topics from the point of ingestion.
- Soft (filter in app) → shared topic + `tenant_id` field.

---

## Step 3 — Requirement → Elimination

Each answer kills entire paths. State what is eliminated, not what "sounds right".

| Requirement | Eliminates | Mandates |
|---|---|---|
| GDPR right-to-erasure | Tombstones, topic deletion, no erasure | Crypto-shredding per `customer_id` (not per tenant) |
| PII in payload | PLAINTEXT, SASL/PLAIN, no Schema Registry | mTLS or OAuth/OIDC · field encryption |
| Exactly-once on critical path | At-most-once, at-least-once | Transactional producers + `read_committed` consumers |
| RPO < 60 seconds | No DR, MirrorMaker 2 | Cluster Linking |
| Cloud-managed only | OSS self-managed, Confluent Platform self-managed | Confluent Cloud |
| Shared platform, independent teams | No Schema Registry, NONE compatibility, auto-register | FULL_TRANSITIVE + CI/CD gate |
| 1,000+ services / independent teams | Shared credentials, soft isolation | mTLS or OAuth/OIDC · hard quotas per principal |
| Immutable audit trail | Short retention, NONE compatibility | Long retention (tiered storage) · FULL_TRANSITIVE |
| Compacted state topic | Tiered storage (incompatible) | Separate events topic (delete) + state topic (compact) |

---

## Step 4 — Layer-by-Layer Walkthrough

Walk these in order once the elimination pass is done.

1. **Ingestion** — Direct SDK producer / Kafka Connect managed / Connect self-managed / CDC (Debezium)
2. **Transport** — Topic structure (gates above), partition key, retention policy
3. **Processing** — Stateless (SMT / simple consumer) or stateful (Kafka Streams / Flink / ksqlDB)? If the problem implies both a real-time and a historical view: one unified pipeline (Kappa / Data Streaming Platform), not two (Lambda) — see Lambda vs Kappa below.
4. **Consumption** — One consumer group per downstream system; offset commit strategy; DLQ
5. **Governance** — Schema Registry compatibility mode; RBAC / ACLs scoped per principal; GitOps
6. **Operations** — DLQ on every connector; consumer lag growth-rate alert; heartbeat topic

---

## Step 5 — Processing Framework Quick Pick

Three questions, answer in order:

1. **Stateless only?** → Stop. Kafka Connect SMT or simple consumer. No framework.
2. **CEP or temporal join?** → Flink only (CEP library). No other option.
3. **State size?**
   - < 20 GB → Kafka Streams (if JVM co-deployed) or ksqlDB (if SQL-first)
   - 20 GB – 1 TB → Kafka Streams + S3 pre-seeding, or Flink
   - > 1 TB → Flink only (`EmbeddedRocksDBStateBackend` + incremental checkpoints)

**Windowed jobs:** use event-time (not processing-time) if output drives reporting, billing, or compliance. Size `max_lateness` from p99 produce→broker delay on source topics.

---

## Anti-Patterns — Never Do These

| Anti-pattern | Correct framing |
|---|---|
| One topic per event type for a stateful entity | Single topic with `event_type` field, keyed by entity ID |
| Shared source topic for B2B tenants needing isolation | Per-tenant source topics from ingestion; ACLs are topic-level, not record-level |
| Fan-out from shared source to "isolate" tenants | Shared source still has commingled PII; route at write time |
| `retention.ms` = short window with tiered storage enabled | `local.retention.ms` = hot window; `retention.ms` = max window |
| Producer dual-writes to events topic + state topic | Stream processor writes to state topic only |
| "Increasing partitions is expensive" | Correct: ordering breaks (key remapping), not computational cost |
| Erasure key at tenant granularity | Erasure key per data subject (`customer_id`), not per `client_id` |
| EOS on every output regardless of criticality | Apply EOS only where duplicates cause irreversible harm (payments, audit) |
| Processing-time windows for billing or compliance | Non-deterministic on replay; switch to event-time with watermarks |

---

## Declare Decided vs Open

End every design section explicitly:

- **Decided** — requirement forced this; no further discussion.
- **Open** — two viable options remain; state what spike would resolve it and what the spike validates.

Never pick without acknowledging uncertainty, and never defer without stating what resolves it.

---

*Full frameworks: [streaming-design-approach.md](streaming-design-approach.md) · [decision-framework.md](decision-framework.md) · [topic-design-framework.md](topic-design-framework.md) · [stream-processing-framework.md](stream-processing-framework.md) · [Lambda vs Kappa](01-Core-Concepts/lambda-vs-kappa-vs-streaming-platform.md) · [Capacity Scaling Cadence](13-Performance-Tuning/capacity-scaling-cadence.md)*
