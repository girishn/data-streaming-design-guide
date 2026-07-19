# Flink Lab 03 — Event-Time Windowing, Watermarks, and a Kafka Streams Comparison

**Mode:** Hands-on (Cloud for Flink) + Hands-on (Local for the Kafka Streams comparison)
**Time:** 2–2.5 hours
**Primary reference:** [06-Stream-Processing/windowing.md](../06-Stream-Processing/windowing.md), [06-Stream-Processing/kafka-streams-vs-flink.md](../06-Stream-Processing/kafka-streams-vs-flink.md)

## Objective

Implement all four window types against the same out-of-order event stream — first in Flink SQL against Confluent Cloud, then the tumbling case again in a local Kafka Streams topology — and directly observe the behavior differences `windowing.md` and `kafka-streams-vs-flink.md` describe rather than taking them on faith: watermark-driven window closing, late-event handling, and the state-size cost of hopping/session windows.

## Prerequisites

- A running Confluent Cloud Flink compute pool + Dedicated cluster (reuse from `flink-lab-02`, or provision fresh per that lab's steps 1–3)
- Local: Java 17+, Maven or Gradle, a local Kafka broker (`docker compose` with a single-broker Confluent Platform image is sufficient)

## Part 1 — Flink SQL: Four Window Types on the Same Stream

### Build a deliberately out-of-order event generator

Produce events to a topic `device_pings` where roughly 1 in 5 events is delayed relative to its `event_time` field by 10–40 seconds — simulate real network jitter rather than perfectly ordered test data. A short Python producer script using `confluent_kafka` is the fastest way to do this; randomize the delay per event before sending.

### Tumbling window

```sql
CREATE TABLE device_5min_counts AS
SELECT device_id, window_start, window_end, COUNT(*) AS ping_count
FROM TABLE(
  TUMBLE(TABLE device_pings, DESCRIPTOR(`$rowtime`), INTERVAL '5' MINUTES)
)
GROUP BY device_id, window_start, window_end;
```

### Hopping window (labeled HOP in Flink SQL)

```sql
CREATE TABLE device_10min_hop_5min AS
SELECT device_id, window_start, window_end, COUNT(*) AS ping_count
FROM TABLE(
  HOP(TABLE device_pings, DESCRIPTOR(`$rowtime`), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES)
)
GROUP BY device_id, window_start, window_end;
```

Confirm in the output that a single ping near a boundary appears in two overlapping window rows — this is the `size/advance` state multiplication `windowing.md` warns about, made concrete.

### Session window

```sql
CREATE TABLE device_sessions AS
SELECT device_id, session_start, session_end, COUNT(*) AS ping_count
FROM TABLE(
  SESSION(TABLE device_pings, DESCRIPTOR(`$rowtime`), INTERVAL '2' MINUTES)
)
GROUP BY device_id, session_start, session_end;
```

Send a burst of pings for one device, then stop entirely for 3 minutes, then send more. Confirm two distinct sessions are emitted, not one — and confirm the session gap threshold you set actually determines the split.

### Watermark strategy and late-event handling

Set an explicit bounded-out-of-order watermark and re-run the tumbling query, then deliberately send one event whose `event_time` falls well inside a window that has already closed:

```sql
SET 'sql.tables.scan.watermark.emit.strategy' = 'on-periodic';
```

Observe what happens to the late event — is it dropped, or does it trigger a retraction/correction, per the mechanism described in `windowing.md`'s "Late Event Handling" section? Record which behavior your configuration actually produced; do not assume it matches the doc without checking the output table.

## Part 2 — Kafka Streams: The Same Tumbling Window, Locally

Write a minimal Kafka Streams topology (Java, `TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))` or an equivalent grace-period variant) that performs the same 5-minute tumbling count over `device_pings`, running against your local broker.

Compare against the Flink output:

- Does your Kafka Streams topology use event-time or processing-time by default? (Check your `TimestampExtractor` configuration — this is the most common place this comparison goes wrong.)
- Kill the Kafka Streams process mid-run and restart it. Time how long state recovery takes versus how instantaneous your earlier Confluent Cloud Flink checkpoint-based recovery would be at this trivial state size — the point of this comparison at small scale is to *notice there's no visible difference yet*, which is exactly what `kafka-streams-vs-flink.md`'s "State Rebuild" section says should hold true below ~20GB.

## Teardown

Delete the Flink compute pool and Dedicated cluster if not reused from another lab. Stop the local Docker Compose stack.

## Validation

- Your hopping window output shows overlap counts matching `size/advance` (10min/5min = 2x) for pings near boundaries.
- Your session window correctly splits on the configured gap, not on a fixed wall-clock boundary.
- You can point to the specific config line where your watermark's `max_lateness`-equivalent value is set, and explain what happens to an event that arrives after it.
- You can state, from direct observation rather than the doc, why Kafka Streams recovery time was not noticeably different from Flink checkpoint recovery at this state size — and what state size would need to change before that stopped being true.

## What You Should Be Able to Explain Afterward

The difference between a late event being *dropped silently* and *triggering a retraction* — and why choosing between them is a business decision (does downstream care about a corrected number arriving after the fact?), not a framework limitation.
