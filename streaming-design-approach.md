# Streaming System Design — Problem Framing Approach

The decision framework ([decision-framework.md](decision-framework.md)) maps requirements to forced choices. This document is the step before that: how to get from an ambiguous problem statement to a set of requirements worth running through the framework.

Applicable to system design interviews, client discovery sessions, and internal architecture reviews.

---

## The Core Mistake

Jumping to architecture before asking questions. The moment you propose a solution without understanding sources, scale, SLAs, and constraints, you are designing for an assumed problem — not the actual one. Ambiguous questions are intentional: they test whether you chase clarity or chase solutions.

---

## Phase 1 — Discovery: Ask Before Designing

Domain knowledge is not required if you ask the right shape of questions. Every streaming design problem has the same underlying structure regardless of industry.

### Sources
- Where does the data live today? Which systems own it?
- Are those systems cloud or on-premises? Internet-reachable or behind private networks?
- Is the data already event-driven, or is it batch/poll today?

### Sinks
- Who consumes this data downstream and what do they need from it?
- How many independent systems consume the same data stream?
- Do consumers need raw events, aggregated state, or both?

### Latency and Scale
- What is the current lag and what is the business target?
- How many events per day — thousands, millions, billions?
- What is the consequence of a one-hour pipeline outage — revenue-impacting or operational inconvenience?

### Constraints
- Cloud provider and existing infrastructure?
- Compliance requirements — PII, financial data, data residency, right-to-erasure?
- First streaming deployment or existing Kafka experience in-house?

These questions are domain-agnostic. You are not asking about the client's industry — you are asking about sources, sinks, latency, scale, and constraints. The answers translate the domain for you.

---

## Phase 2 — Elimination: Requirements Kill Solution Paths

This is the key shift in thinking. Each answer from Phase 1 eliminates entire solution paths before you ever reach tradeoffs. You are not selecting the right answer — you are eliminating the wrong ones.

| Requirement | What It Eliminates |
|---|---|
| On-premises source system | Managed connectors — cannot reach systems behind private networks |
| First Kafka deployment | Self-managed brokers — operational overhead too high for a new team |
| Multiple independent consumer teams | No schema governance — silent breakage on first schema change is guaranteed |
| PII in the data | No erasure strategy — compliance failure under GDPR, Privacy Act, or equivalent |
| 1-hour RTO acceptable | Cross-region DR — adds cost and complexity without satisfying the stated SLA |
| Near-real-time acceptable | Exactly-once semantics — adds coordination overhead with no business benefit |
| Millions of events per day | Basic/Standard cluster tiers — insufficient throughput and missing private networking |

The residual — what survives all eliminations — is your design. You are not choosing Confluent Cloud Dedicated because it sounds right. You are choosing it because every alternative is eliminated by a specific requirement. This distinction matters: you can defend a design built on elimination; a design built on intuition collapses under the first follow-up question.

---

## Phase 3 — Structure: Layer-by-Layer Walkthrough

Once elimination has narrowed the space, walk through the design in a fixed layer order. This sequence works for any streaming system:

**1. Ingestion** — How does data enter the stream?
- Direct SDK producer instrumented at the source (lowest latency, cleanest design)
- Kafka Connect — managed for cloud sources, self-managed for on-premises
- CDC (Debezium) for database change capture without application changes

**2. Transport** — Topic design
- Partition key: what determines ordering and co-location?
- Retention: time-based delete for event logs; compaction for latest-value state tables
- Separate event stream topics (every change) from state topics (current position)

**3. Processing** — Is transformation required?
- Stateless: filter, enrich, route — any Connect SMT or lightweight consumer
- Stateful: aggregation, joins, windowing — Kafka Streams (embedded, simple) or Flink (separate cluster, powerful)
- No processing needed: raw events are sufficient for all consumers

**4. Consumption** — Consumer topology
- One consumer group per downstream system — independent pace, independent offset
- Consumers own deduplication; do not commit offset until processing succeeds
- Separate consumer groups for operational consumers (low latency) vs analytical consumers (high throughput)

**5. Governance** — Schema and access control
- Schema Registry with an explicit compatibility mode per topic — non-negotiable for multi-team platforms
- RBAC: scoped service principal per consumer, read-only access to exactly the topics they need
- GitOps: ACLs and topic configuration declared in Terraform, applied on PR merge — no ticket-based access requests

**6. Operations** — Failure modes and observability
- Dead letter queue on every connector — a bad event must not stall the entire feed
- Consumer lag growth-rate alert, not absolute threshold
- Connector availability monitoring — a failed connector is a silent data gap
- End-to-end heartbeat: a synthetic event produced periodically, measured at the consumer end

---

## Phase 4 — Declare What Is Decided vs Open

A complete design is not the goal. Structured thinking under uncertainty is. At each phase, be explicit:

- **Decided** — requirements force this choice, no further discussion needed
- **Open** — more information required; state what you would spike and what the spike would validate

An open item handled correctly looks like: *"SAP connectivity has two viable mechanisms — JDBC polling and CDC via Debezium. I'd treat this as a spike: validate DB access and licensing for JDBC first since it's simpler; fall back to CDC if that's blocked. The connector design doesn't change either way."*

An open item handled incorrectly looks like: picking one option without acknowledging the uncertainty, or deferring without stating what would resolve it.

---

## What Domain Knowledge Actually Gives You

Domain knowledge speeds up Phase 1 — you ask better questions faster because you already know what matters in the domain. In retail, you know that reference data (product catalogue, store master) needs compaction not delete; that PII is in customer data not inventory records; that store systems are typically on private networks.

Without domain knowledge, structured discovery questions reach the same place — it takes longer and requires the interviewer or client to fill gaps you would otherwise anticipate. The questions are the skill. Domain knowledge is an accelerant, not a prerequisite.

---

## The Pattern

```
Ambiguous problem statement
          ↓
Phase 1: Discovery questions
(sources → sinks → latency/scale → constraints)
          ↓
Phase 2: Requirement → elimination map
(each answer kills solution paths)
          ↓
Residual options = candidate design
          ↓
Phase 3: Layer-by-layer walkthrough
(ingest → transport → process → consume → govern → operate)
          ↓
Phase 4: Declare decided vs open
```

---

## Cross-References

- Full elimination map by dimension — [decision-framework.md](decision-framework.md)
- Worked example: enterprise banking platform — [14-Case-Studies/banking-platform-design.md](14-Case-Studies/banking-platform-design.md)
- Worked example: enterprise retail platform — [14-Case-Studies/retail-platform-design.md](14-Case-Studies/retail-platform-design.md)
