# Broker-Side Schema ID Validation

## What It Does

Broker-side schema ID validation is a server-side interceptor that runs during the produce request path, before a record is appended to the partition log. For every record, the broker verifies:

1. The record contains the Confluent wire format magic byte (`0x00`)
2. The 4-byte schema ID extracted from the header is registered in the Schema Registry
3. The schema ID is assigned to the target topic's subject (under the configured naming strategy)

If any check fails, the broker rejects the write with a serialisation error and the record never enters the log. Downstream consumers are protected without any changes to consumer code.

Without this feature, schema enforcement is entirely client-side. A producer with a misconfigured serialiser, a buggy schema registration step, or a deliberate bypass can write arbitrary bytes to any topic. The first symptom is typically a consumer crashing with `Unknown Magic Byte` or a deserialisation exception hours or days later.

## Tier Requirements and Configuration

Broker-side validation requires a **Confluent Enterprise licence**:
- **Confluent Cloud:** Dedicated clusters and above (not available on Basic or Standard)
- **Confluent Platform (self-managed):** Confluent Server (not OSS Kafka brokers)

**Broker-level configuration** — point the broker at the Schema Registry:
```properties
confluent.schema.registry.url=https://schema-registry-host:8081
```

**Topic-level configuration** — enable per topic:
```bash
# Enable value schema validation
kafka-configs.sh --alter --topic orders \
  --add-config confluent.value.schema.validation=true

# Enable key schema validation (optional)
kafka-configs.sh --alter --topic orders \
  --add-config confluent.key.schema.validation=true
```

Enable validation selectively on topics where schema correctness is critical — high-value event streams, compliance topics, and topics with many consumers. Enabling it broadly without reviewing producer configurations first will surface existing misconfigurations as immediate write rejections.

## The Problem It Solves

The canonical failure mode in Kafka Connect and microservice pipelines is a producer that sends raw JSON, plain strings, or a schema-less byte payload to a topic that consumers expect to be Avro or Protobuf encoded.

The Confluent Avro wire format begins with `0x00` (magic byte). Raw JSON begins with `{` (`0x7B`). When a JSON-deserialising consumer receives an Avro record, or vice versa, the first byte does not match expectations — the consumer throws `SerializationException: Unknown magic byte`.

Without broker-side validation, this record is permanently in the log. Every consumer that processes it will fail at deserialisation. The only remediation options are:
- Skip the record (at-most-once, data loss)
- Route it to a DLQ (requires consumer-side error handling)
- Replay the topic from before the bad record (expensive, affects all consumers)

With broker-side validation enabled, the bad record is rejected at write time. The producer receives an error; the log remains clean; consumers are never exposed to the malformed record.

## Observability Model — Producer-Side, Not Consumer-Side

This is the critical operational distinction from the DLQ pattern.

With a Dead Letter Queue, a bad record enters the log and is routed to the DLQ topic. The consumer can inspect it, understand why it failed, and remediate. The signal is consumer-side.

With broker-side validation, the record is rejected before it enters the log. The producer receives a hard write error. The record is permanently discarded — no DLQ entry, no error message in the consumer, no trace. From the consumer's perspective the message simply never existed.

| Mechanism | Bad record fate | Producer signal | Consumer signal |
|---|---|---|---|
| DLQ (application-layer) | Enters log → DLQ topic | No error | DLQ depth metric, inspectable record |
| Broker-side validation | Rejected before log entry | Hard write exception | None — record never existed |

**Consequence:** broker-side validation must be paired with **producer write error rate alerting**. If a producer is sending records that fail schema validation, the only signal is the producer's exception and the broker's rejection metric. Consumers have no visibility. Without producer error monitoring, a schema misconfiguration that silently rejects thousands of records per minute will not surface until a consumer notices missing data — which may be hours later.

**Monitor on the producer side:**
- Producer `record-error-rate` JMX metric — increases on write rejection
- Confluent Cloud Metrics API: `kafka.producer.record_error_rate` per producer principal
- Alert threshold: any non-zero sustained error rate on a production topic

## Limitations

**No payload introspection:** the broker validates that the schema ID exists and is assigned to the topic. It does not decode or validate the payload against the schema. A producer that correctly embeds a valid schema ID but serialises the payload incorrectly (wrong field values, wrong Avro encoding) will pass validation. Payload correctness remains a client-side responsibility.

**Tombstones are never rejected:** records with null values (tombstones) are used for log compaction key deletion. The broker does not reject null-value records even if schema validation is enabled, because tombstones are a valid operational pattern that exists outside the schema contract.

**Validation overhead:** each produce request triggers a schema ID lookup against the Registry. The broker caches validated IDs to minimise latency impact — subsequent produces with the same schema ID to the same topic hit the cache without a Registry round-trip. Under high throughput with a stable schema set, the overhead is negligible.

**Does not prevent wrong-but-valid schema IDs:** if a producer registers a schema ID from a different topic's subject and writes records using it, the validation will pass if the ID itself is a registered schema. The assignment check is against the subject derived from the topic name and the configured naming strategy — misconfigured RecordNameStrategy subjects can create gaps. See the subject interaction section below.

## Decision — Broker-Side Validation vs Consumer DLQ vs Both

These are not alternatives — they guard against different failure types. A mature platform runs all three layers simultaneously.

| Layer | Mechanism | Failure type | Record preserved? |
|---|---|---|---|
| 1 — Broker | Broker-side schema ID validation | Wrong wire format, unregistered schema ID | No — rejected before log |
| 2 — Serializer | CEL quality rules (Data Contracts) | Semantic violations — bad values, invalid enums, missing fields | Yes — routed to DLQ |
| 3 — Consumer | Consumer-side DLQ | Transient failures, downstream unavailability, business logic errors | Yes — routed to DLQ |

### Use Broker-Side Validation When

**Compliance and audit log topics** — records that enter the log may be legally significant. Kafka has no delete-record guarantee at scale. A malformed audit record that enters the log is harder to remediate than one that never entered. Reject at the gate.

**Compacted topics** — a bad record with a valid key becomes the latest compacted state for that key indefinitely. A malformed payload or accidental tombstone corrupts the reference state for every consumer until a correct record overwrites it. Broker-side validation prevents log corruption on compacted topics.

**High fan-out topics** — one bad record without broker-side validation produces N DLQ entries, N alerts, and N investigations — one per consuming team. Reject once at the broker instead.

**Multi-tenant platform topics** — one team's misconfigured producer should not pollute a shared topic that other teams depend on. Broker-side validation protects all consumers from one bad producer.

**ML and analytics feeder topics** — bad records in training data cause silent model degradation for days or weeks. Hard rejection at produce time surfaces the issue immediately at the source.

### Use Consumer-Side DLQ When

**Legacy or external producers** — you cannot guarantee the Confluent Schema Registry serialiser is used. The producer may not embed a schema ID at all. Broker-side validation would reject everything; consumer-side DLQ gives visibility into what arrived.

**Semantic failures** — broker-side validation has no payload introspection. A record with a valid schema ID but `order_total = -500` or an undeclared enum value passes broker-side validation. CEL quality rules (layer 2) or consumer DLQ (layer 3) must catch these.

**Transient processing failures** — the record is valid. The consumer's downstream database is temporarily unavailable. This is a consumer failure, not a data quality failure. DLQ + exponential backoff + re-drive is the correct pattern.

**Schema evolution rollout windows** — producer is on v3, consumers are on v1. Migration rules handle the transform. During the cutover window, consumers may encounter records they cannot yet process. DLQ with re-drive after consumer upgrade is the recovery path.

**Records requiring inspection and remediation** — DLQ preserves the bad record with full headers: failure reason, source topic, partition, offset. An ops team can inspect, fix, and re-produce. Broker-side rejection discards the record permanently.

**Basic or Standard Confluent Cloud tiers** — broker-side validation requires Dedicated. Consumer DLQ is the only structural option on lower tiers.

**Early-stage topics** — hard rejections during development slow iteration. DLQ gives visibility into what producers are actually sending before schema contracts are locked.

### Decision Flow

```
What kind of failure am I defending against?
              │
              ▼
Wrong wire format / unregistered schema ID?
         Yes ─► Broker-side validation
              │
              ▼
Valid schema ID but bad field values / business rule violation?
         Yes ─► CEL quality rules (Data Contracts) → DLQ
              │
              ▼
Valid record, consumer processing failed?
         Yes ─► Consumer-side DLQ + retry
```

### When to Use Only Consumer DLQ (No Broker-Side Validation)

- Producer is a legacy system, external partner, or IoT device not using the Confluent SR client
- Topic intentionally carries mixed formats (union schema, multiple event types)
- Cluster is Basic or Standard tier
- Topic is in active development — hard rejections impede iteration; DLQ gives faster feedback

### When to Layer All Three

Standard recommendation for production platform topics with multiple consumers:

1. **Broker-side validation on** — structural gate, keeps the log clean
2. **CEL quality rules registered** — semantic gate, violations routed to DLQ for inspection
3. **Consumer DLQ configured** — transient and business logic failures handled at the consumer

Each layer has a distinct responsibility. None replaces the other.

## Subject Context Interaction

The broker's validation behaviour depends on the subject naming strategy configured on the producer:

**`TopicNameStrategy`:** the broker searches for the schema ID across all schema contexts to find a subject associated with the target topic. This is forgiving — a schema ID registered in a non-default context will still validate if it is associated with the correct topic subject.

**`RecordNameStrategy`:** the broker only searches the **default schema context**. If the producer registered the schema in a named context (e.g., `:.internal:`) rather than the default context, the validation fails even if the schema ID is technically valid. Producers using RecordNameStrategy must ensure schemas are registered in the default context, or validation will reject valid records.

Verify context alignment when enabling validation on topics that use RecordNameStrategy — misalignment causes production write failures that look identical to genuinely invalid schema IDs.

## Cross-References

- Broker-side validation vs consumer DLQ decision — [05-Enterprise-Connect/error-handling-dlq.md](../05-Enterprise-Connect/error-handling-dlq.md)
- CEL quality rules (semantic layer, complements structural validation) — [08-Stream-Governance/data-contracts.md](data-contracts.md)
- Schema compatibility modes — [08-Stream-Governance/schema-evolution.md](schema-evolution.md)
- Broker-side validation as a producer onboarding gate — [10-Operational-Patterns/producer-onboarding.md](../10-Operational-Patterns/producer-onboarding.md)
- Consumer onboarding observability trade-off — [10-Operational-Patterns/consumer-onboarding.md](../10-Operational-Patterns/consumer-onboarding.md)
