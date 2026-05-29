# Connector Onboarding — Production Access Gates

A Kafka Connect connector is simultaneously a producer (source connectors) or consumer (sink connectors) and a long-running infrastructure component managed by the platform team. It has different failure modes from application producers and consumers — a stalled connector silently stops data flow, and a misconfigured SMT chain corrupts every record in the topic.

These gates apply to both source (ingest) and sink (egress) connectors. Differences are noted per gate.

---

## Gate 1 — Connector Type and Hosting Decision

**Source connectors** read from an external system and write to a Kafka topic.
**Sink connectors** read from a Kafka topic and write to an external system.

**Checklist:**
- Managed Confluent connector or self-managed (CFK on EKS)? Managed connectors cannot reach systems on private corporate networks or on-premises. Self-managed is required if the source/sink is behind a VPN or PrivateLink boundary. See `09-Security-Architecture/private-networking.md`
- Connector plugin version pinned — not `latest`. Plugin version is locked in the connector config and only updated through a reviewed change
- Task count declared based on source system throughput and partition count of the target topic. For source connectors: tasks ≤ partition count of the target topic, otherwise idle tasks waste resources
- Connector name follows platform naming convention: `{direction}-{system}-{domain}` (e.g., `source-sap-inventory`, `sink-postgres-orders`)

---

## Gate 2 — Schema and SMT Chain Review

The platform team reviews the SMT (Single Message Transform) chain before deployment. SMT bugs corrupt every record in the topic — there is no rollback once records are written.

**Checklist:**
- Schema for the target topic (source connectors) registered in Schema Registry before the connector starts. The connector must not use `auto.register.schemas=true` in production
- SMT chain documented and reviewed: each transform in the chain, its configuration, and its expected input/output. Common pitfalls: `ReplaceField` silently dropping fields, `RegexRouter` misconfiguration routing to wrong topic
- SMT chain validated in staging with a representative sample of source records — including edge cases (null fields, unexpected formats from the source system)
- For CDC connectors (Debezium): snapshot vs streaming mode decision documented. Snapshot mode on a large table blocks the source database. Initial snapshot should run in a maintenance window or use `snapshot.mode=schema_only` for tables that can tolerate missing historical data. See `10-Operational-Patterns/cdc-debezium.md`

---

## Gate 3 — Dead Letter Queue — Mandatory

A connector without a DLQ stalls on the first record it cannot process. In a source connector, this stops the entire data feed. In a sink connector, this blocks the partition.

**Checklist:**
- DLQ topic provisioned: `{connector-name}.dlq`
- `errors.deadletterqueue.topic.name` configured on the connector
- `errors.deadletterqueue.context.headers.enable=true` — DLQ records include headers with the failure reason, connector name, and original topic/partition/offset for forensic investigation
- `errors.tolerance=all` configured — connector continues processing other records when one record fails
- DLQ depth alert configured — a growing DLQ with no alert is silent data loss
- Team has a documented runbook for inspecting DLQ records, correcting the source data or transform, and re-driving records through the connector

---

## Gate 4 — Credential Injection

Connectors require credentials to connect to external systems (database passwords, API keys, service account tokens). These must never appear in connector config files, git repositories, or Kubernetes Secrets in plaintext.

**Checklist:**
- Credentials stored in AWS SSM Parameter Store (or equivalent secrets manager)
- Injected at pod startup via CSI Secrets Provider — not mounted as Kubernetes Secrets or set as environment variables in the connector config
- Credentials rotated on a schedule documented and tested — connector restart on rotation validated in staging
- Service account used by the connector has minimum required permissions on the source/sink system — not an admin account

See `10-Operational-Patterns/gitops-terraform.md` for the CSI SecretProviderClass pattern.

---

## Gate 5 — Throughput and Resource Declaration

**Checklist:**
- Peak records/second and average record size declared — platform uses this to validate task count and set connector-level quotas
- For source connectors: ingress quota applied to the connector's service principal before go-live
- For sink connectors: egress quota applied; sink system capacity validated — a fast Kafka consumer overwhelming a slow database is a common failure mode
- `max.poll.interval.ms` reviewed for sink connectors with slow downstream systems — same risk as application consumers. See `10-Operational-Patterns/consumer-onboarding.md`
- Connector task count tuned: too few tasks = throughput bottleneck; too many tasks = resource waste and source system overload

---

## Gate 6 — Monitoring and Alerting

Connector availability monitoring is the platform team's responsibility at the cluster level. Connector-specific alerting is the owning team's responsibility.

**Checklist:**
- Connector status alert configured: `connector.status = FAILED` triggers a P2 incident for source connectors feeding operational topics (inventory, orders). A failed source connector is a silent data gap
- Task-level failure alert configured — a connector can be RUNNING while individual tasks are FAILED
- Source connector: lag between source system change time and Kafka produce time monitored (end-to-end latency)
- Sink connector: consumer lag on the source topic monitored — lag growth indicates the sink is falling behind
- Integration with the team's existing alerting platform (Datadog, PagerDuty, ServiceNow) confirmed before go-live

---

## Cross-References

- Managed vs self-managed connectors — [05-Enterprise-Connect/managed-connectors.md](../05-Enterprise-Connect/managed-connectors.md)
- SMT configuration — [05-Enterprise-Connect/single-message-transforms.md](../05-Enterprise-Connect/single-message-transforms.md)
- DLQ patterns — [05-Enterprise-Connect/error-handling-dlq.md](../05-Enterprise-Connect/error-handling-dlq.md)
- CDC and Debezium — [10-Operational-Patterns/cdc-debezium.md](cdc-debezium.md)
- Private networking for self-managed connectors — [09-Security-Architecture/private-networking.md](../09-Security-Architecture/private-networking.md)
- Credential injection via CSI — [10-Operational-Patterns/gitops-terraform.md](gitops-terraform.md)
- Consumer lag monitoring — [11-Monitoring-Observability/consumer-lag.md](../11-Monitoring-Observability/consumer-lag.md)
- Platform automation model — [10-Operational-Patterns/platform-automation.md](platform-automation.md)
