# Windowing — Time-Based Aggregation Semantics

## The Four Window Types

Windowing groups a continuous stream of events into finite buckets for aggregation. The four types differ in how bucket boundaries are defined and how records are assigned to them.

### Tumbling Windows

Fixed-size, non-overlapping, contiguous intervals. Every record belongs to exactly one window.

```
Events:  e1  e2  e3  e4  e5  e6  e7  e8
         |----window 1----|----window 2----|
         t=0             t=5             t=10  (5-minute windows)
```

Use when: periodic reporting (hourly revenue, 5-minute error rate, daily aggregates). The simplest window type — no state duplication, predictable output cadence.

### Hopping Windows

Fixed-size, overlapping intervals defined by a **window size** and an **advance interval**. Every record belongs to `size / advance` windows simultaneously.

```
Window size: 10 min, Advance: 5 min
Window 1: [0, 10)
Window 2: [5, 15)
Window 3: [10, 20)
An event at t=7 belongs to Window 1 and Window 2
```

Use when: moving averages, rolling metrics where you want output more frequently than the window size. The overlap factor (`size / advance`) multiplies state size — a 10-minute window advancing every 1 minute means each record contributes to 10 concurrent windows.

### Sliding Windows

Overlapping windows where the boundary is defined by the time span between records, not by a fixed grid. A window exists for every pair of events within the time span.

Use when: detecting that two events occurred within N milliseconds of each other — latency SLA violations, fraud detection (two charges within 30 seconds), proximity detection. Sliding windows generate a large number of window instances and are expensive to maintain for high-cardinality keys.

### Session Windows

Dynamic windows based on **activity gaps**. A session window opens on the first event for a key and extends as long as events keep arriving within the inactivity gap. The window closes when no event arrives for the configured gap duration.

```
Gap: 30 min
Events: e1(t=0) e2(t=10) e3(t=25) ... [silence] ... e4(t=80) e5(t=95)
Session 1: [0, 25]  (closed after 30 min of silence)
Session 2: [80, 95] (new session)
```

Use when: user session analytics, clickstream aggregation, IoT device activity bursts. Session windows have unpredictable duration — a user who clicks continuously for 4 hours produces a single enormous window. Size state stores accordingly and set a maximum session duration if unbounded sessions are a risk.

## Event-Time vs Processing-Time

**Processing-time** windows are defined by the wall clock of the stream processor at the moment a record is received. Simple to implement — no coordination needed. Non-deterministic: the same events replayed at a different time produce different window assignments. Ingestion delay (a consumer that fell behind and is catching up) causes events to land in the wrong window.

**Event-time** windows are defined by a timestamp embedded in the record itself — when the event actually occurred, not when it was processed. Deterministic: replaying the same events always produces the same windows. Correctly handles out-of-order delivery and late-arriving events.

For any aggregation that must be correct — revenue totals, compliance reporting, SLA measurement — use event-time. Processing-time is acceptable only for approximate, operational metrics where exact window assignment does not matter.

## Watermarks

With event-time processing, the system needs to know when it is safe to close a window and emit results — it cannot wait forever for late-arriving events. **Watermarks** are the mechanism that signals progress in event time.

A watermark at time `W` asserts: "all events with timestamp `t < W` have been received." When the watermark advances past a window's end boundary, the window is considered complete and results are emitted.

**Watermark strategies:**

- **Bounded out-of-order watermark:** `watermark = max_seen_event_time - max_lateness`. Waits a fixed amount of time for late arrivals before closing windows. The `max_lateness` value is a latency/completeness trade-off — larger values catch more late events at the cost of delaying window output.
- **Monotonically increasing watermark:** assumes events arrive in order; watermark = last seen event time. Zero latency, zero tolerance for out-of-order events.

In Flink:
```java
WatermarkStrategy
    .<MyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, ts) -> event.getEventTimestamp());
```

In Kafka Streams, watermarks are implicit — the stream time advances with the maximum timestamp seen across all partitions. Grace periods serve the equivalent function.

## Late Event Handling

Events that arrive after the watermark has passed their window's end boundary are **late events**. Three strategies:

**Grace period / allowed lateness:** extend the window's active period by a fixed duration after the watermark passes. Events arriving within the grace period update the window and trigger a result update (a retraction of the previous result followed by the corrected value, or an incremental update depending on the framework). After the grace period expires, the window state is discarded.

```java
// Flink: 5-minute tumbling window with 30-second grace period
TumblingEventTimeWindows.of(Time.minutes(5)).withLateness(Time.seconds(30))
```

**Drop:** events arriving past the grace period are silently discarded. Appropriate when late events are rare and approximate results are acceptable. Monitor the late-events-dropped metric to confirm the assumption holds.

**DLQ routing:** route late events to a dead letter topic for manual reconciliation or offline reprocessing. Use when late events represent genuine data quality issues that need investigation rather than expected out-of-order delivery.

## Window Trade-offs Summary

| Window type | State overhead | Output cadence | Use case |
|---|---|---|---|
| Tumbling | Low (1 window per key per interval) | Fixed, predictable | Periodic reports, batch-equivalent aggregations |
| Hopping | Medium–high (`size/advance` × tumbling) | Frequent, overlapping | Moving averages, rolling metrics |
| Sliding | High (window per event pair) | Per event | Event proximity detection, latency tracking |
| Session | Variable (unbounded duration) | On inactivity gap | User sessions, device activity bursts |
