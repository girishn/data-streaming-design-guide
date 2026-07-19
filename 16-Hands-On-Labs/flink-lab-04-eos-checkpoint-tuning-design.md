# Flink Lab 04 — Exactly-Once Sink Design and Checkpoint Tuning

**Mode:** Design (no infrastructure)
**Time:** 45 minutes
**Primary references:** [stream-processing-framework.md](../stream-processing-framework.md) (Layer 6), [07-Advanced-Reliability/exactly-once-semantics.md](../07-Advanced-Reliability/exactly-once-semantics.md)

## Objective

Given an RTO target and a duplicate-tolerance constraint, size a Flink checkpoint interval and choose EOS vs at-least-once — then write the actual job configuration that implements the decision. This closes the loop on the EOS behavior you should have observed hands-on in `flink-lab-03` (if you haven't done that lab yet, do it first — this exercise is much less useful in the abstract).

## Scenario

A Flink job consumes from a `payment_events` topic and writes settlement records to a downstream `settlement_ledger` topic that a batch reconciliation job reads once per hour. Two constraints from the business:

1. **RTO target: 90 seconds.** If the job fails, it must resume processing within 90 seconds of the failure being detected.
2. **Duplicate tolerance: zero.** A duplicate settlement record double-counts revenue in the reconciliation job and requires a manual finance correction — this has happened before and is why this constraint exists.

## Task

### Step 1 — Checkpoint interval

Using the RTO table in `stream-processing-framework.md` Layer 6 (Q8), determine which checkpoint interval band the 90-second RTO target falls into. Write the actual `checkpointInterval` value you'd configure, and the `min.pause.between.checkpoints` value derived from the 30% rule stated in the same section. Show your arithmetic.

### Step 2 — EOS or at-least-once

Apply the rule from Layer 6: "Apply EOS only on paths where duplicates cause irreversible business harm." Given the zero-duplicate-tolerance constraint and the stated history of manual finance corrections, decide EOS or at-least-once for this specific sink. Justify in two sentences — one should reference why at-least-once-with-idempotent-consumer is *not* a sufficient substitute here (consider: does the downstream batch reconciliation job in this scenario have a natural idempotency key, or would building one be the actual mitigation, and is that cheaper than EOS?).

### Step 3 — Write the configuration

Produce the Flink job configuration snippet implementing your decision:

```java
// Fill in: checkpoint interval, checkpointing mode, min pause between checkpoints
env.enableCheckpointing(/* ? */);
env.getCheckpointConfig().setCheckpointingMode(/* ? */);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(/* ? */);
```

State what sink property is required for the EOS decision to actually hold end to end — reference `exactly-once-semantics.md`'s description of two-phase commit sink requirements. Confirm the Kafka sink you're targeting (`settlement_ledger`) meets that requirement natively, per `kafka-streams-vs-flink.md`'s note on which sinks support 2PC out of the box.

### Step 4 — Stress-test your own decision

A colleague proposes reducing the checkpoint interval to 5 seconds "to be extra safe" beyond your Step 1 answer. Using the "Overhead" column in the RTO table, write two sentences explaining the concrete operational cost of that change and why it doesn't actually improve the RTO target you were given.

## Validation

- Your checkpoint interval falls within the correct RTO band and your `min.pause.between.checkpoints` arithmetic matches the 30% rule exactly — not "roughly."
- Your EOS justification names the *specific* failure mode (duplicate settlement → double revenue count → manual correction) rather than a generic "financial data should always be exactly-once" — the framework explicitly warns against applying EOS everywhere by default.
- Your Step 4 answer identifies that shorter checkpoint intervals reduce data-to-reprocess on failure but do not by themselves reduce *detection-to-resume* time, which is the actual bottleneck in most RTO scenarios — if your answer conflates "less to reprocess" with "faster to reprocess," revisit the distinction.

## What You Should Be Able to Explain Afterward

Why "financial data" is not by itself a sufficient justification for exactly-once — and what the actual test is (does a duplicate cause irreversible harm that idempotent consumption can't absorb cheaply) that should drive the decision on any given sink, financial or not.
