# Module 08 — Stream Governance

Schema evolution rules, broker-side enforcement, PII compliance patterns, and the three pillars of Confluent Stream Governance.

## Files

- [schema-evolution.md](schema-evolution.md) — Wire format, compatibility modes and upgrade sequencing, safe vs breaking changes in Avro and Protobuf, subject naming strategy decision (TopicNameStrategy vs RecordNameStrategy vs TopicRecordNameStrategy), union type pattern for ksqlDB/Flink compatibility, and registry best practices.
- [broker-side-validation.md](broker-side-validation.md) — Schema ID validation at the broker, tier requirements, per-topic configuration, limitations, and subject context interaction.
- [pii-tracking.md](pii-tracking.md) — Field-level PII tagging, the crypto-shredding pattern for right-to-erasure, KMS integration, and limitations.
- [stream-catalog.md](stream-catalog.md) — Data discovery, business metadata, tag-based governance policies, and the distinction from Schema Registry.
- [stream-lineage.md](stream-lineage.md) — End-to-end data flow visualization, automated map construction, engineering use cases, and limitations.
