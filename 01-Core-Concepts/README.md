# Module 01 — Core Concepts

Foundational positioning for engineers evaluating or operating Kafka and Confluent. Covers the capability delta between OSS Kafka and Confluent Cloud (what you get vs what you build yourself), the architectural shift from batch ETL to real-time event streaming, and the log as a first-class data structure — the conceptual foundation behind all design decisions in later modules.

## Files

- [kafka-vs-confluent.md](kafka-vs-confluent.md) — OSS Kafka vs Confluent Cloud capability map, cost crossover heuristics, the "gold-layer" broker-as-system-of-record model.
- [data-streaming-platform.md](data-streaming-platform.md) — Event-driven architecture primitives, the immutable log model, and the architectural implications of treating data in motion as the primary substrate.
- [lambda-vs-kappa-vs-streaming-platform.md](lambda-vs-kappa-vs-streaming-platform.md) — Why teams end up with Lambda's dual-pipeline operational tax, the Kappa "batch is bounded streaming" insight and its two standard objections, and how the Data Streaming Platform model (shift-left + open-table materialization) resolves both. Comparison table and greenfield-vs-migration guidance.
