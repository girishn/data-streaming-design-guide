# Flink Lab 05 — Connect vs Flink Pipeline Boundary Design Exercise

**Mode:** Design (no infrastructure)
**Time:** 60–75 minutes
**Primary reference:** [connect-vs-flink-framework.md](../connect-vs-flink-framework.md)

## Objective

Work three scenarios through `connect-vs-flink-framework.md`'s layered decision path end to end —
origin/destination, stateless vs stateful, joins, CDC handling, latency scoping, exactly-once,
schema evolution, and multi-team sharing — and produce a stage-by-stage pipeline diagram (which
hop is Connect, which is Flink) plus the concrete configuration each choice implies. This is the
exercise to run before an app-team requirements meeting: it forces you to place every pipeline
stage on the correct side of the integration/processing boundary instead of defaulting to "Flink
can do everything," or conversely trying to make Connect do a join.

## Method

For each scenario, answer every relevant layer's question explicitly, in order, before drawing a
conclusion — do not skip straight to an architecture diagram. Write your answer as: a **stage-by-stage
pipeline diagram** (source → [Connect or Flink] → ... → sink) naming which tool owns each hop, plus
for any Flink stage — **join type** (if any), **checkpoint/EOS setting**, **schema evolution
handling**, and **isolation mode** (if multi-team) — and a two-to-three sentence justification
citing the specific layer that forced each placement.

Check your reasoning against the "Decision Sequence Summary" flowchart in
`connect-vs-flink-framework.md` after each scenario — do not check it before.

## Scenario A — Real-Time Fraud Signal Pipeline

A payments platform's fraud team wants a pipeline: CDC the `transactions` table from the orders
Postgres database, enrich each transaction with the customer's current risk tier (from a separate
`customers` table, updated a few times a day), and publish the enriched, scored event to a new
`transactions-enriched` topic. Two other teams — risk reporting and customer support — also want to
consume `transactions-enriched` once it exists. A duplicate enriched event reaching the fraud
model is tolerable (it just re-scores); a duplicate reaching risk reporting's running total is not.
The team has been told "sub-second" but nobody has said sub-second relative to what.

Work through: What does "CDC the transactions table" map to in Layer 1? Is the customer risk-tier
enrichment a stateless lookup, or does Layer 3's join table apply — and if so, which row? What does
"consumed by three teams" force in Layer 9? Given the asymmetric duplicate-tolerance between the
fraud model and risk reporting, does the whole pipeline need one EOS setting, or does Layer 7 let
you split it? What's the first question you'd ask back about "sub-second" before designing
anything?

## Scenario B — Marketing Attribution Export

A marketing team has a `clickstream` topic already flowing in Kafka (produced directly by the web
app, no CDC involved). They want IP addresses and email fields masked, and want the result landed
nightly in the marketing data warehouse for attribution modelling. They've asked whether "we should
just write a Flink job, since attribution modelling is complex." No joins, no aggregation — every
event is processed independently, and the destination only needs a batch load, not continuous
streaming.

Work through: Does this reach Layer 2 as stateful at all? What does the framework's Layer 1
anti-pattern entry say about this exact request? Given the destination is a nightly warehouse load,
does "we should just write a Flink job" survive Layer 4 and Layer 6 regardless of the Layer 2
answer? What would you actually tell the marketing team to build instead?

## Scenario C — Inventory Aggregation with Evolving Schema

A logistics company CDCs a `product_catalog` table (schema changes roughly weekly — new attributes
get added as merchandising adds product lines) and needs a continuously-updated count of in-stock
units per warehouse, exposed on a new `warehouse-stock-levels` topic that both the replenishment
team and the public-facing store-locator API's backing service will consume. The platform team has
been burned before: a previous Flink job silently stopped reflecting a new `warehouse_region` field
for three weeks after it was added to the source table, and nobody noticed until a report looked
wrong.

Work through: What does the weekly schema-change cadence, combined with the aggregation
requirement, force in terms of framework layers touched (both Layer 3 and Layer 8)? Given the past
incident, what specifically should the platform team commit to operationally so this doesn't happen
silently again — is this a config fix or a process fix? Does "two other teams will consume this
topic" change anything about where schema-compatibility enforcement should sit, per Layer 9?

## Validation

For each scenario, confirm your written answer against these checks:

- **Scenario A** should place the CDC ingestion (both `transactions` and `customers`) on Connect
  (Debezium), the enrichment as a Flink temporal join against a compacted `customers` topic (Layer
  3, stream-to-table row), and the final publish back into Kafka via Flink's `KafkaSink` — not
  through a Connect sink, since the destination is a topic. On EOS: the framework doesn't mandate
  one setting for the whole pipeline — Layer 7 is about which *hop* needs the guarantee, and here
  the duplicate-tolerance is asymmetric per *consumer*, not per producer, which a single publish
  step can't split on its own. The right answer is to publish once with `read_committed`-safe
  delivery and push the tolerance difference onto each consumer (risk reporting does its own
  idempotent aggregation) rather than trying to make the producer "exactly-once for one consumer,
  at-least-once for another." If your answer tried to split EOS at the source, revisit Layer 7's
  `read_committed` discussion. On "sub-second": the first question back should be Layer 6's — until
  it lands in Kafka, until the fraud model scores it, or until risk reporting's dashboard reflects
  it? These have different owners and different achievable numbers.
- **Scenario B** should terminate at Layer 2 with "Kafka Connect SMT, no framework needed" for the
  masking step — the same anti-pattern named in `stream-processing-framework.md`'s Scenario C
  (flink-lab-01), now with a Connect sink attached instead of a bare consumer. The Flink suggestion
  should be rejected on two independent grounds: Layer 2 (no state at all — masking is per-record)
  and Layer 4/Layer 6 (a nightly batch warehouse load has no latency floor a streaming engine
  improves on). If your answer kept Flink "for the masking, since attribution modelling is
  complex," you've conflated the downstream analytics complexity with this pipeline stage's actual
  complexity — the masking step is trivial regardless of what marketing does with the data
  afterward.
- **Scenario C** should force both Layer 3 (a windowed/keyed aggregation — stateful, belongs in
  Flink) and Layer 8 (the schema-evolution asymmetry) as independent, separately-reasoned findings
  — if your answer only touched one, redo it. The fix for the past incident is explicitly a
  **process fix, not a config fix**: lenient JSON parsing prevents a crash but is exactly what let
  the field go unused silently for three weeks. The actual fix is an operational commitment — a
  schema-change notification path from whoever owns schema registration to whoever operates the
  Flink job, triggering a deliberate stop-with-savepoint / DDL-update / restart. The framework
  explains *why* this happens; it doesn't make it stop happening on its own. On the two-consumer
  question: yes — per Layer 9, this pushes schema compatibility enforcement to the registry
  (`BACKWARD` or `FULL_TRANSITIVE`) as a cross-team contract the moment a second team depends on
  `warehouse-stock-levels` — a breaking change is no longer just this team's problem to catch.

## What You Should Be Able to Explain Afterward

Why Scenario A and Scenario C both involve CDC and both involve Flink, yet land on almost entirely
different sets of concerns — Scenario A is dominated by Layer 7 and Layer 9's consumer-contract
questions, Scenario C by Layer 8's schema-evolution asymmetry. The CDC source is not what
determines which layers matter; the shape of what happens *after* ingestion is.
