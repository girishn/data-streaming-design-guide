# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

This is a pure-markdown reference guide for engineers making production decisions about Apache Kafka and Confluent Platform. The audience is engineers past the tutorial stage — the content assumes familiarity with Kafka basics and targets precise technical decisions: partition strategies, exactly-once guarantees, schema evolution, CDC wiring, security authentication flows.

There is no build system, test suite, or CI pipeline. All content is markdown.

## Module Structure

Sixteen numbered modules, each a self-contained directory:

| # | Module | Status |
|---|--------|--------|
| 01 | Core Concepts — OSS Kafka vs Confluent, the log as data structure | Done |
| 02 | Broker Infrastructure — KRaft, partitioning, replication | Done |
| 03 | Data Production — Idempotent and transactional producers | Done |
| 04 | Data Consumption — Consumer groups, cooperative rebalancing, static membership | Done |
| 05 | Enterprise Connect — Managed connectors, SMTs, DLQ | Done |
| 06 | Stream Processing — Kafka Streams vs Flink, state, RocksDB, windowing | Done |
| 07 | Advanced Reliability — ISR mechanics, exactly-once protocol | Done |
| 08 | Stream Governance — Schema evolution, broker-side validation, PII/crypto-shredding, stream catalog, stream lineage | Done |
| 09 | Security Architecture — mTLS, OAuth/OIDC, CEL Identity Pools, multi-protocol | Done |
| 10 | Operational Patterns — Transactional outbox, Debezium CDC, RocksDB S3 pre-seeding, GitOps, OPA, migration | Done |
| 11 | Monitoring and Observability — Consumer lag, broker metrics, Confluent Cloud Metrics API | Done |
| 12 | Multi-Region and Disaster Recovery — Cluster Linking, MirrorMaker 2, active-active, RPO/RTO | Done |
| 13 | Performance Tuning — Producer batching, consumer fetch tuning, broker thread configuration | Done |
| 14 | Case Studies — B2B logistics CX, financial e-commerce EOS payment ledger, fraud detection, streaming RAG | Done |
| 15 | AI and GenAI Patterns — Streaming RAG lifecycle, Flink ML_PREDICT(), airline chatbot case study | Done |
| 16 | Hands-On Labs — Flink framework selection, Confluent Cloud Flink SQL, EOS/checkpoint tuning, mTLS/OAuth design, CFK local mTLS+OAuth, RBAC/Identity Pools Terraform, SCRAM-to-OAuth migration | Done |

Each module has a `README.md` listing its files with one-line descriptions, and individual `.md` files for each topic.

## Root-Level Framework Files

Five meta-documents at the repo root are entry points for cross-cutting design work. They sit outside any module and link into module detail:

| File | Purpose |
|------|---------|
| `decision-framework.md` | Phase-by-phase elimination flow: maps hard requirements (compliance, SLA, scale) to forced choices across infrastructure, delivery guarantee, processing, governance, and security. Start here for greenfield designs. |
| `streaming-design-approach.md` | Discovery-first problem framing: the structured question sequence (sources, sinks, latency, compliance, ownership) that precedes the decision framework. Use for interviews and client discovery sessions. |
| `topic-design-framework.md` | Layered decision path for topic topology: ordering constraints and isolation first, then retention and state topology, partition sizing, schema governance, and anti-pattern checklist. |
| `stream-processing-framework.md` | Framework selection path: stateless vs stateful → Kafka Streams vs Flink vs ksqlDB → state size, time semantics, windowing, fault tolerance configuration. Assumes data is already in Kafka. |
| `connect-vs-flink-framework.md` | Pipeline boundary path for requests touching an external source/sink: where Kafka Connect (integration) hands off to Flink (processing), and how CDC, latency SLAs, exactly-once, schema evolution, and multi-team sharing shift that boundary. Precedes `stream-processing-framework.md` when an external system is involved. |

## Content Conventions

**File naming:** `kebab-case.md`, descriptive and specific (e.g., `cooperative-rebalancing.md`, not `rebalancing.md`).

**Depth target:** Production-decision level. Each file should answer "what do I choose and why?" with the tradeoffs that matter at scale — not a tutorial walkthrough. See `01-Core-Concepts/kafka-vs-confluent.md` as the reference for tone and depth.

**Structure within files:** Use H2 sections. Lead with what the technology/feature is (concisely), then decision factors, cost/tradeoff tables where useful, and concrete configuration implications. Avoid padding with definitions that the audience already knows.

**Cross-references:** When a file depends on a concept in another module, link explicitly by relative path. The guide is non-linear — readers jump to a specific problem area, so cross-references must be self-sufficient.

**No prescribed reading order:** The `README.md` describes this as a reference, not a course. Don't write content that requires previous modules to be read first.

## Key Architectural Concept (The "Gold-Layer" Model)

The central design principle this guide advocates: the **broker is the system of record**, not the database. Topics are immutable, append-only logs; databases are materialized views rebuilt from topic replay. Schema changes govern at the topic level (Schema Registry), and databases follow. This inversion must be reflected consistently across all modules — design questions start with "what is the topic structure?" not "what table do we need?"

## Mock Interview Workflow

When the user asks for a mock interview or system design session:

1. Use Excalidraw to draw the architecture diagram incrementally as the conversation progresses — reveal components as the candidate (user) discovers them, not all at once.
2. At the end of the session, save both the completed case study `.md` file and the `.excalidraw` diagram file to the appropriate module directory (usually `14-Case-Studies/` or the relevant topic module).
3. Provide a gap analysis at the end: what concepts were missed or shallowly covered, mapped to the specific module files that address them.

## Ingest workflow

The RAG system behind this project is a NotebookLM notebook with ID `be527226-52ed-48d3-b4b2-ea02fe61856f`.

1. Query NotebookLM for inputs to create the docs in the project. While querying ensure that the number of calls and number of tokens is optimized to not to exhaust the notebooklm rate limits.
2. Discuss key takeaways with the user before writing anything.

## Question answering

When the user asks a question:

1. Read existing repo first to find relevant pages
2. Read those pages and synthesize an answer
3. Cite specific pages in your response
4. If the answer is not present in the project, say so clearly.
5. If the answer is not in the docs in the project, make a notebook query.
6. If answer is found in the notebook, provide the answer and update the docs in the project.
7. If the answer is not found in the notebook, do a web search and find the most relevant and recent resource for that and provides it.
8. Based on the web search provide the answer and also the source.
9. Check if the new source is worth adding to the notebook.
10. If it is worth adding, let the user know and user will manually add it.
11. If it is not worth adding, discard the new source, update the docs in the project log and remember the decision.