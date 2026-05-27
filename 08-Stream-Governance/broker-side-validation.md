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

## Limitations

**No payload introspection:** the broker validates that the schema ID exists and is assigned to the topic. It does not decode or validate the payload against the schema. A producer that correctly embeds a valid schema ID but serialises the payload incorrectly (wrong field values, wrong Avro encoding) will pass validation. Payload correctness remains a client-side responsibility.

**Tombstones are never rejected:** records with null values (tombstones) are used for log compaction key deletion. The broker does not reject null-value records even if schema validation is enabled, because tombstones are a valid operational pattern that exists outside the schema contract.

**Validation overhead:** each produce request triggers a schema ID lookup against the Registry. The broker caches validated IDs to minimise latency impact — subsequent produces with the same schema ID to the same topic hit the cache without a Registry round-trip. Under high throughput with a stable schema set, the overhead is negligible.

**Does not prevent wrong-but-valid schema IDs:** if a producer registers a schema ID from a different topic's subject and writes records using it, the validation will pass if the ID itself is a registered schema. The assignment check is against the subject derived from the topic name and the configured naming strategy — misconfigured RecordNameStrategy subjects can create gaps. See the subject interaction section below.

## Subject Context Interaction

The broker's validation behaviour depends on the subject naming strategy configured on the producer:

**`TopicNameStrategy`:** the broker searches for the schema ID across all schema contexts to find a subject associated with the target topic. This is forgiving — a schema ID registered in a non-default context will still validate if it is associated with the correct topic subject.

**`RecordNameStrategy`:** the broker only searches the **default schema context**. If the producer registered the schema in a named context (e.g., `:.internal:`) rather than the default context, the validation fails even if the schema ID is technically valid. Producers using RecordNameStrategy must ensure schemas are registered in the default context, or validation will reject valid records.

Verify context alignment when enabling validation on topics that use RecordNameStrategy — misalignment causes production write failures that look identical to genuinely invalid schema IDs.
