# Testing Strategy — Unit, Integration, Contract, and Soak Testing for Streaming Applications

## The Testing Layers

A streaming application needs four distinct layers of testing, each catching a different class of bug. Skipping a layer doesn't just leave a gap — it shifts that bug's discovery to production.

| Layer | Validates | Runs against | Speed |
|---|---|---|---|
| Unit | Topology/processor logic in isolation | No broker | Milliseconds, runs in every CI build |
| Integration | Real serialization, rebalancing, retention, compaction | Ephemeral real broker | Seconds to low minutes |
| Contract | Producer/consumer schema and semantic agreement | Schema Registry (real or mocked) | Seconds, runs pre-merge |
| Acceptance / Soak | End-to-end behavior and sustained-load stability | Real staging cluster | Minutes to hours, post-deploy |

## Unit Testing — `TopologyTestDriver`

For Kafka Streams topologies, `TopologyTestDriver` runs the topology in-process with no broker, no network, and no Kafka cluster dependency — the standard tool for fast, CI-friendly logic tests:

```java
TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);
TestInputTopic<String, Order> inputTopic = testDriver.createInputTopic(
    "orders", stringSerializer, orderSerializer);
TestOutputTopic<String, Enriched> outputTopic = testDriver.createOutputTopic(
    "enriched-orders", stringDeserializer, enrichedDeserializer);

inputTopic.pipeInput("order-1", order);
assertEquals(expected, outputTopic.readValue());
```

**Known limitations — do not treat this as sufficient for production sign-off:**
- Simulates the topology as if it has exactly **one partition**. Bugs that only appear with multiple partitions — partition-dependent joins, co-partitioning mismatches, key distribution issues — will not surface here.
- No RocksDB caching or commit-interval behavior — output timing in the test does not reflect real commit-interval batching in production.
- No real network, no rebalancing, no broker-side compaction or retention effects.

Use it for what it's good at: processor logic, transformation correctness, edge cases in business rules. It is not a substitute for integration testing.

## Integration Testing — Testcontainers

Anything that depends on real broker behavior — rebalancing, retention, compaction, actual wire-format serialization, Schema Registry interaction — needs a real (if ephemeral) broker. **Testcontainers** is the standard tool: it starts a disposable Kafka broker (and Schema Registry, if needed) in Docker for the duration of the test run, then tears it down.

```java
@Testcontainers
class OrderEnrichmentIntegrationTest {
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    // Test against kafka.getBootstrapServers() — a real broker, isolated per test run
}
```

Container startup adds seconds per test class — mitigate by sharing one container across all test classes in a suite rather than starting a fresh one per test. Reserve integration tests for what unit tests structurally cannot catch (multi-partition behavior, actual serde roundtrips, rebalance-triggered reprocessing); don't duplicate topology-logic assertions that `TopologyTestDriver` already covers faster.

**Alternative for Spring shops:** `spring-kafka-test`'s `EmbeddedKafkaBroker` runs an in-process broker instead of a container — faster startup, but it's a real (if lightweight) broker instance, not a mock, so the same "what does this catch that unit tests don't" reasoning applies.

## Contract Testing

On a platform with independently deployed producer and consumer teams, schema compatibility mode (`08-Stream-Governance/schema-evolution.md`) catches structural breaks at registration time, but it doesn't catch semantic breaks — a field whose *meaning* changed while its type stayed compatible. Consumer-driven contract testing (e.g., Pact) closes that gap: the consuming team publishes the exact shape and semantics they depend on, and the producer's CI pipeline verifies against it before deploy. This matters most for shared, cross-team topics where the producer has no direct visibility into every downstream consumer's assumptions — see the multi-team sharing concerns in `connect-vs-flink-framework.md` Layer 9.

## Acceptance and Soak Testing

Unit and integration tests run pre-merge against synthetic data. Acceptance and soak tests run post-deploy against a real staging cluster with realistic load, and are what actually validates the "ready for production" claim:

- **Acceptance:** functional correctness against a real cluster — the staging equivalent of `producer-onboarding.md` Gate 6 and `consumer-onboarding.md` Gate 1's staging validation steps.
- **Soak:** sustained load over an extended window (hours, not minutes) to surface issues that only appear over time — RocksDB compaction backlog, slow memory growth, connection pool exhaustion, gradual consumer lag drift under realistic throughput.

Run both automatically after every deployment, not manually and not only before major releases — regressions introduced by routine changes are exactly what a soak test is positioned to catch that a fast CI suite structurally cannot (by design, CI runs don't sustain load long enough to surface drift).

**This is a separate pipeline from the platform's own topic/schema/connector provisioning pipeline** (`gitops-terraform.md`, `platform-automation.md`) — that pipeline ends at `terraform apply` and a notification; it doesn't own application deploys. Acceptance and soak testing is triggered by *each team's own* application CI/CD, on their own deploy cadence, against the already-provisioned topics/schemas. Don't look for a test-trigger stage in the platform pipeline — it isn't there because it isn't that pipeline's job.

## Anti-Patterns

| Anti-pattern | Signal | Fix |
|---|---|---|
| Treating `TopologyTestDriver` green as production-ready | No multi-partition or rebalance testing anywhere in the suite | Add Testcontainers integration tests before first production deploy |
| Integration tests running against a shared dev cluster | Flaky, order-dependent test failures; cross-team topic pollution | Ephemeral Testcontainers broker per test run |
| No contract test between producer and consumer on a shared topic | Schema Registry compatibility passes but a downstream team's logic breaks anyway | Consumer-driven contract tests (e.g., Pact) for cross-team topics |
| Soak testing only before major releases | Regressions from routine changes ship undetected between releases | Automated soak test triggered on every deploy, not gated to release events |

## Cross-References

- Schema compatibility modes (structural, not semantic, contract enforcement) — [08-Stream-Governance/schema-evolution.md](../08-Stream-Governance/schema-evolution.md)
- Data contracts and CEL quality rules — [08-Stream-Governance/data-contracts.md](../08-Stream-Governance/data-contracts.md)
- Producer staging validation gate — [10-Operational-Patterns/producer-onboarding.md](producer-onboarding.md) Gate 6
- Consumer schema contract gate — [10-Operational-Patterns/consumer-onboarding.md](consumer-onboarding.md) Gate 1
- Multi-team schema-as-contract concerns — [connect-vs-flink-framework.md](../connect-vs-flink-framework.md) Layer 9
- Kafka Streams topology and state store fundamentals — [06-Stream-Processing/state-management.md](../06-Stream-Processing/state-management.md)
