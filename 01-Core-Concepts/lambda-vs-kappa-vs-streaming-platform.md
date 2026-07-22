# Lambda vs Kappa vs Data Streaming Platform

## The Shared Model

Every data pipeline, batch or streaming, is a directed graph of **source → transform → sink**: data enters from somewhere (a database, a Kafka topic, an API), business logic filters/aggregates/enriches/deduplicates it, and the result lands somewhere (a warehouse, a dashboard, another topic). This framing is useful precisely because it's architecture-agnostic — Lambda, Kappa, and the Data Streaming Platform model are three different answers to "how many of these graphs do I run, and on how many engines?" not three different data models.

## Lambda Architecture

**What it is:** two parallel pipelines feeding one serving layer — a **batch layer** (Spark/Snowflake jobs, orchestrated by Airflow/Dagster/Argo, producing the long-term historical view) and a **speed layer** (Kafka + Flink or Spark Streaming, producing the real-time delta). A serving layer stitches the two views together for consumers.

**How teams end up here:** almost never by deliberate greenfield design. The typical path is an existing batch pipeline that works fine until a stakeholder asks for a real-time dashboard, the batch job can't just be scheduled to run faster, and a full redesign isn't on the table. Bolting a streaming pipeline alongside the existing batch one is the pragmatic-looking choice in the moment.

**Why it costs more than it looks like it costs:**

| Cost | Mechanism |
|---|---|
| Duplicated business logic | The same transform (dedup, enrichment, aggregation) is implemented twice — once in the batch engine's dialect, once in the streaming engine's — and the two rarely stay in sync as requirements evolve |
| Two tech stacks, often two teams | A "big data"/batch team and a streaming team, each owning a different codebase, different deployment pipeline, different on-call rotation |
| Consistency debugging | When the real-time dashboard and the batch report disagree — which they will — resolving it means diffing two independently-implemented pipelines to find where the divergence happened. "Eventual consistency" in practice often means *inconsistent until someone manually reconciles it* |
| Different failure modes | The batch and speed layers fail independently, in different ways, with different recovery procedures — doubling the operational surface area |

Lambda is not a design mistake — it powers real production systems at scale, and it's a legitimate stopgap when a full re-architecture genuinely isn't feasible on the required timeline. The point isn't that it's wrong; it's that the ongoing operational tax is the price of not unifying, and that price compounds every time the business logic changes.

## Kappa Architecture

**Core insight:** batch is just a bounded stream. An unbounded stream has a start and no end; a bounded stream (a batch) has a start and an end. If the processing engine can handle both, there is no structural reason to run two separate pipelines — treat batch as streaming-with-a-finish-line, and use one engine for everything.

**What it looks like:** every source — databases via CDC (Debezium), logs via agents, external systems via their own connectors — flows into one unified stream in a broker (Kafka). One processing engine (Flink, or Kafka Streams for JVM-native cases) transforms it and sends the result downstream, whether that's other streaming consumers or a batch-style sink. One codebase, one tech stack, one team, one place where business logic lives and gets debugged.

**The two standard objections, and why they're solved, not fundamental:**

1. **"I can't afford to keep terabytes of history in my broker."** True when streaming data had to live entirely on the broker's local disks. Tiered storage (KIP-405, shipped as a production-ready OSS feature in Kafka 3.6) moves cold segments to object storage, decoupling retention cost from local disk cost — see `02-Broker-Infrastructure/tiered-storage.md`.
2. **"My analysts and BI tools can't query a Kafka stream."** Partially solved, not eliminated, by Kappa alone: Flink SQL and Spark Structured Streaming let engineers query streams directly, but most BI tools and query engines (Snowflake, Trino, Athena) still have no native Kafka client — this is the gap the Data Streaming Platform model closes.

## The Data Streaming Platform

The Data Streaming Platform model is Kappa plus two refinements that address what Kappa leaves open:

**Shift left.** Move transforms — deduplication, schema enforcement, data contracts, PII masking — upstream into the stream itself, so every downstream consumer reads already-clean, already-governed data instead of each one re-implementing the same cleanup independently. This is the same principle `08-Stream-Governance/data-contracts.md` and `10-Operational-Patterns/producer-onboarding.md` (via its CEL quality rules) already apply at the topic-contract level — shift-left isn't a separate idea bolted onto Kappa, it's Kappa's governance layer taken seriously.

**Materialize into an open table format.** Sync the stream into Apache Iceberg (or another open table format) so tools with no native Kafka client get a queryable table backed by the same underlying data — no separate warehouse-loading pipeline required. On Confluent Cloud this is Tableflow (`01-Core-Concepts/kafka-vs-confluent.md`): automated topic-to-Iceberg materialization, queryable via Trino, Spark, Athena, Snowflake.

**Net result:** one stream, two consumption modes from the same underlying data — real-time, for Flink jobs and downstream microservices; and tabular, for whatever query engine the analytics side already uses. Nobody builds a second pipeline to get there.

This is the architecture `01-Core-Concepts/data-streaming-platform.md` describes in depth — the log as the primary substrate, databases and tables as materialized views derived from it, not the other way around.

## Comparison

| | Lambda | Kappa | Data Streaming Platform |
|---|---|---|---|
| Pipelines | 2 (batch + speed) | 1 | 1 |
| Tech stacks / teams | Usually 2 | 1 | 1 |
| Business logic | Duplicated | Single implementation | Single implementation, enforced upstream |
| Historical query access | Native (batch layer is a warehouse) | Requires stream-native tooling or replay | Native, via open-table materialization |
| BI-tool / SQL-engine compatibility | Native (batch side) | Limited (Flink SQL / Spark Structured Streaming only) | Native (Iceberg/Tableflow) |
| Primary failure mode | Two pipelines silently diverging | Storage cost and non-streaming-tool access, if unaddressed | Materialization lag between stream and table |

## Which to Choose

**Greenfield:** default to the Data Streaming Platform model. This matches the guide's own Gold-Layer position (`01-Core-Concepts/kafka-vs-confluent.md`, `01-Core-Concepts/data-streaming-platform.md`): design the topic structure and schema first, treat databases and tables as derived views, not the system of record. There is no scenario where starting with Lambda is the better greenfield choice — it's a pattern you arrive at by patching, not one you should design toward.

**Already running Lambda:** the cost of unifying is real (re-implementing batch business logic in the streaming engine, migrating historical data into the stream's retention model or backfilling via CDC), so treat it as a scoped migration, not a rewrite mandate. The trigger to actually do it is usually the first time a consistency bug between the two layers costs real engineering time to debug — at that point the ongoing tax has already exceeded the one-time migration cost.

**When Lambda might still be the pragmatic short-term call:** a hard deadline for a real-time view where the streaming engine genuinely cannot yet handle the batch side's transform complexity (a heavy ML feature pipeline doing full historical joins, for instance), and where the plan is explicitly to unify later rather than to leave two pipelines running indefinitely. The failure mode isn't choosing Lambda under time pressure — it's never coming back to unify it.

## Cross-References

- The log-as-substrate model this architecture builds toward — [01-Core-Concepts/data-streaming-platform.md](data-streaming-platform.md)
- OSS vs Confluent capability gap, including Tableflow — [01-Core-Concepts/kafka-vs-confluent.md](kafka-vs-confluent.md)
- Tiered storage mechanics and KIP-405 — [02-Broker-Infrastructure/tiered-storage.md](../02-Broker-Infrastructure/tiered-storage.md)
- CDC into the unified stream — [10-Operational-Patterns/cdc-debezium.md](../10-Operational-Patterns/cdc-debezium.md)
- Shift-left governance in practice (CEL data quality rules, schema contracts) — [08-Stream-Governance/data-contracts.md](../08-Stream-Governance/data-contracts.md)
- Choosing the processing engine once you're past this decision — [stream-processing-framework.md](../stream-processing-framework.md)
- Where an external source/sink shifts this decision — [connect-vs-flink-framework.md](../connect-vs-flink-framework.md)
