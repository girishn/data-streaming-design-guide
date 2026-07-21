# Producer Onboarding — Production Access Gates

Producers own the topic contract. Every consumer on the platform inherits the schema, quality rules, and partitioning decisions a producer makes at onboarding time. Getting this wrong is expensive — schema changes cascade to every consumer, and partition key choices are effectively permanent.

These gates apply before the producer writes a single event to production.

---

## Gate 1 — Topic Design Review

The producer team owns the topic specification. The platform team validates it against platform standards before provisioning.

**Checklist:**
- Topic name follows the platform naming convention: `{domain}.{entity}.{event-type}.v{N}` (e.g., `payments.transaction.initiated.v1`). The name drives ACL derivation — non-compliant names break automation
- Partition count justified against throughput target and expected max consumer concurrency — not defaulted. See `02-Broker-Infrastructure/partitioning-strategies.md` for sizing formula and practical bands
- Retention policy declared: `delete` for event streams, `compact` for latest-value state tables. Blanket `delete` on reference data topics is a design error
- Replication factor = 3 with `min.insync.replicas=2` — non-negotiable for production
- Topic configuration reviewed for edge cases: `max.message.bytes` if payloads are large, `message.timestamp.type` if event-time ordering matters downstream

**Why:** partition count and key design are irreversible decisions on a live topic. Changing partition count post-launch requires a full blue-green topic migration — see `10-Operational-Patterns/blue-green-topic-migration.md`.

---

## Gate 2 — Schema Registration and Contract Authorship

The producer team registers the schema and authors the data contract before the first produce.

**Checklist:**
- Avro schema registered in Schema Registry under `{topic-name}-value` (TopicNameStrategy default)
- Compatibility mode declared explicitly — `FULL_TRANSITIVE` for topics shared across domain teams; `BACKWARD_TRANSITIVE` acceptable for domain-internal topics
- `auto.register.schemas=false` on the producer — schema registration happens through the CI/CD pipeline, not at runtime
- Data quality rules authored in CEL for all semantically critical fields: non-null identifiers, positive amounts, valid enums, format patterns. These are the contract consumers rely on
- PII and PCI fields tagged in schema metadata (`"PII"`, `"PCI"` tags) — triggers CSFLE encryption mandate and Stream Catalog governance policies
- Ownership metadata declared: team name, email, SLA, data domain
- Migration rules (JSONata) declared for any future breaking change path the team anticipates

**Why:** the producer is the schema author. If they defer CEL quality rules to "later," consumers receive no protection against semantic breaks. See `08-Stream-Governance/data-contracts.md`.

---

## Gate 3 — Partition Key and Ordering Contract

The partition key decision must be documented as part of the topic contract — it is a commitment to every consumer.

**Checklist:**
- Partition key declared and documented: what entity does it represent, and what ordering guarantee does it provide consumers?
- Key distribution analysed: hot key risk assessed. If a small number of keys generate disproportionate volume (e.g., a high-traffic merchant ID), document mitigation (compound key, custom partitioner, or accepted hot partition)
- For topics where multiple producer instances will run: single-producer-per-key guarantee established — either via Kafka Streams (one task per partition) or consistent hashing at the application router layer. See `02-Broker-Infrastructure/partitioning-strategies.md` — concurrent producer ordering problem
- Transactional producer required if the producer writes to multiple partitions atomically (e.g., payment + ledger update in one transaction): `transactional.id` configured, `enable.idempotence=true`

---

## Gate 4 — Producer Configuration Review

**Checklist:**
- `enable.idempotence=true` — mandatory for all producers. Prevents duplicate writes on retry
- `acks=all` — mandatory for production topics
- `compression.type` declared — `lz4` for throughput-optimised topics, `zstd` for storage-optimised topics. Uncompressed production topics are not permitted above 1 MB/s sustained throughput
- `linger.ms` and `batch.size` tuned to the throughput profile — not left at defaults. See `13-Performance-Tuning/producer-tuning.md`
- For PII fields: CSFLE encryption executor configured and tested in staging — KMS access validated before first production write. A producer without KMS access that attempts to write tagged PII fields fails the contract rule and routes to DLQ
- Throughput declaration: peak events/second and average payload size declared — platform uses this to validate partition count sufficiency and set ingress quota

---

## Gate 5 — ACL and Principal Provisioning

**Checklist:**
- Service principal scoped to `WRITE` on the specific topic — not wildcard
- `CREATE` permission on transactional ID if EOS producer
- Schema Registry `WRITE` access on the subject — scoped to the team's own schemas only
- DLQ topic provisioned alongside the main topic (`{topic-name}.dlq`) — even producers need a DLQ destination for contract rule failures
- Ingress quota applied to the producer principal before go-live

---

## Gate 6 — Staging Validation

**Checklist:**
- Producer has successfully written 1,000+ events to the staging equivalent of the topic
- Schema Registry rejected at least one intentionally malformed event (contract rule validation confirmed active)
- CSFLE encryption confirmed — a consumer without KMS access sees ciphertext in staging
- Transactional produce tested end-to-end in staging if EOS is required
- Downstream consumer in staging has consumed and processed the staging events without error

For the full testing layer breakdown behind this gate (unit, integration, contract, acceptance/soak) — see `10-Operational-Patterns/testing-strategy.md`.

---

## Cross-References

- Topic design and partition count — [02-Broker-Infrastructure/partitioning-strategies.md](../02-Broker-Infrastructure/partitioning-strategies.md)
- Concurrent producer ordering problem — [02-Broker-Infrastructure/partitioning-strategies.md](../02-Broker-Infrastructure/partitioning-strategies.md)
- Idempotent and transactional producers — [03-Data-Production/idempotent-producers.md](../03-Data-Production/idempotent-producers.md), [03-Data-Production/transactional-producers.md](../03-Data-Production/transactional-producers.md)
- Schema evolution and compatibility — [08-Stream-Governance/schema-evolution.md](../08-Stream-Governance/schema-evolution.md)
- Data contracts and CEL quality rules — [08-Stream-Governance/data-contracts.md](../08-Stream-Governance/data-contracts.md)
- CSFLE for PII field encryption — [08-Stream-Governance/csfle.md](../08-Stream-Governance/csfle.md)
- Producer tuning — [13-Performance-Tuning/producer-tuning.md](../13-Performance-Tuning/producer-tuning.md)
- Consumer onboarding — [10-Operational-Patterns/consumer-onboarding.md](consumer-onboarding.md)
- Platform automation model — [10-Operational-Patterns/platform-automation.md](platform-automation.md)
- Testing strategy: unit, integration, contract, acceptance/soak — [10-Operational-Patterns/testing-strategy.md](testing-strategy.md)
