# Flink Lab 01 — Framework Selection Design Exercise

**Mode:** Design (no infrastructure)
**Time:** 45–60 minutes
**Primary reference:** [stream-processing-framework.md](../stream-processing-framework.md)

## Objective

Work three scenarios through the layered decision path end to end — stateless vs stateful, processing type, state size, team context, time semantics, fault tolerance — and produce a one-paragraph justified recommendation plus the concrete configuration each choice implies. This is the exercise to run before a design review or an interview: it forces you to apply every layer instead of pattern-matching to "Flink because it's newer."

## Method

For each scenario below, answer every layer's question explicitly before moving to the next — do not skip to a conclusion. Write your answer as: **framework**, **state backend** (if applicable), **window type** (if applicable), **checkpoint interval**, **EOS or at-least-once**, and a two-sentence justification citing the specific layer that eliminated the alternatives.

Check your reasoning against the "Decision Sequence Summary" flowchart in `stream-processing-framework.md` after each scenario — do not check it before.

## Scenario A — Real-Time Fraud Scoring

A payments platform needs to flag a transaction if it is followed by a second transaction from the same card within 30 seconds, anywhere in the world. The card-level state (recent transaction history) must be queryable within 200ms of the second transaction arriving. Peak state size is estimated at 400 GB across all active cards. The team is a mix of Python data engineers and Java backend engineers; the fraud model is expected to evolve monthly. A false negative (missed fraud) costs the business materially more than a delayed alert.

Work through: Is this stateless? What processing type does "within 30 seconds of each other" map to in the Layer 2 table? What does 400 GB imply for Layer 3? Does the team context in Layer 4 change your answer or just your language choice? Does this require windowing at all, or is it closer to CEP?

## Scenario B — Hourly Revenue Rollup for Billing

A SaaS platform bills customers based on hourly API usage. The aggregation must be correct even when the ingestion pipeline lags by up to 3 minutes during traffic spikes — a usage spike that arrives late must still land in the correct billing hour, not the hour it happened to be processed in. State per customer is small (a running counter). The team runs a Java monolith and wants the aggregation co-deployed with the billing service's existing deployment pipeline.

Work through: stateless or stateful? Which window type from Layer 5, Q7? What does "correct even when late" force in Layer 5, Q5 — and what does that in turn force for Layer 6 (does billing correctness imply EOS, or does idempotent downstream consumption make at-least-once acceptable)?

## Scenario C — IoT Device Telemetry Enrichment

A logistics company ingests GPS pings from 50,000 delivery vehicles. Each ping needs to be enriched with static reference data (vehicle metadata: model, depot, driver assignment) that changes a few times a day. No aggregation, no windowing — just a lookup and a field append before the enriched event is republished.

Work through: does this even reach Layer 2? What tells you in Layer 1 that this scenario terminates early, and what should you actually build instead?

## Validation

For each scenario, confirm your written answer against these checks:

- Scenario A should force Flink via the Layer 2 CEP row *and* the Layer 3 state-size row independently — if your reasoning only used one of those two signals, redo the exercise. State backend should be `EmbeddedRocksDBStateBackend` with incremental checkpoints; EOS is justified by "duplicates cause irreversible business harm" in Layer 6's table (a duplicate fraud alert is cheap, a duplicate missed-fraud silence is not — think through which failure mode EOS actually protects against here before committing to an answer).
- Scenario B should land on event-time with watermarks (billing correctness), tumbling windows (Layer 5 Q7 "periodic reports" row), and Kafka Streams (Layer 4 "JVM team; co-deployed"row) — not Flink. If you chose Flink, re-read Layer 4's distinction between "processing must scale independently" and "team wants co-deployment."
- Scenario C should terminate at Layer 1 with "Kafka Connect SMT or a simple consumer, no framework needed." If you designed a Flink or Kafka Streams job for this, you've reproduced the guide's named anti-pattern — see the Anti-Pattern Checklist's first row.

## What You Should Be Able to Explain Afterward

Why the same "join a stream to reference data" shape (Scenario C) can be stateless while a superficially similar aggregation (Scenario B) is stateful — the distinction is whether the operation requires memory *across records*, not whether it involves a lookup.
