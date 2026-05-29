# Consumer Onboarding — Production Access Gates

Granting a new team access to a production Kafka topic is an operational decision with lasting consequences. A team that falls behind creates consumer lag that can cascade to other consumers on the same cluster. A team without a DLQ stalls on the first malformed record. A team without lag monitoring becomes your incident at 2am.

These gates apply before ACLs and role bindings are provisioned — not after.

---

## Gate 1 — Schema Contract Acknowledgement

The consuming team must demonstrate that they can deserialise the topic's schema correctly in a non-production environment before getting production access.

**Checklist:**
- Team has downloaded and validated the Avro/Protobuf/JSON Schema from Schema Registry in staging
- Deserialisation succeeds with the current schema version
- Team is configured for `BACKWARD` or `BACKWARD_TRANSITIVE` compatibility on their consumer — they can process historical data or skip multiple schema versions without failure
- Subject naming strategy matches the topic (`TopicNameStrategy` is the default; `RecordNameStrategy` or `TopicRecordNameStrategy` require explicit justification)
- Broker-side schema validation is confirmed active on the topic — consumers will not receive records with unregistered schema IDs

**Why:** the first schema evolution on a live topic will expose any consumer that skipped this gate. Silent deserialization failures are the most common cause of unexplained consumer lag growth.

---

## Gate 2 — Consumer Group Isolation and Quotas

Each consuming team gets a dedicated consumer group and a scoped service principal. No shared consumer groups, no shared credentials.

**Checklist:**
- Consumer group ID follows the platform naming convention: `cg-{team}-{purpose}` (e.g., `cg-loyalty-points-calculator`)
- Service principal created and scoped to read-only access on the specific topics required — not wildcard ACLs
- Byte-rate quota and request-rate quota applied to the service principal before go-live
- Consumer parallelism reviewed: the team's instance count must not exceed the topic's partition count — idle consumers above the partition ceiling waste resources without improving throughput
- `CooperativeStickyAssignor` (`partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor`) configured — prevents stop-the-world rebalances during rolling restarts or scaling events

**Why:** without quotas, a runaway analytical consumer can exhaust broker bandwidth and starve operational consumers. Without `CooperativeStickyAssignor`, a pod restart during peak traffic causes a full group rebalance that stalls all partitions momentarily.

For Kubernetes (StatefulSet deployments): require `group.instance.id` set per pod. Static membership prevents a pod restart from triggering a rebalance — the broker waits for the instance to rejoin within `session.timeout.ms`. See `04-Data-Consumption/static-membership.md`.

---

## Gate 3 — Dead Letter Queue Configuration

A consuming team without a DLQ will stall the moment a malformed record arrives. In a multi-tenant platform, that stall generates lag alerts and support requests.

**Checklist:**
- DLQ topic provisioned: `{source-topic}.{consumer-group}.dlq`
- Consumer is configured to route non-retryable errors (deserialization failures, schema violations, permanent business logic failures) to the DLQ rather than retrying indefinitely
- Team has a documented process for inspecting, correcting, and re-producing records from the DLQ
- Retry logic for transient errors (network timeouts, downstream database unavailability) uses exponential backoff with a maximum retry bound — not an infinite loop
- DLQ depth is included in the team's monitoring alerts — a growing DLQ that nobody is watching is an undetected data loss event

**Why:** the platform cannot control what records producers write. The consumer must degrade gracefully on bad records rather than blocking the entire partition.

---

## Gate 4 — Offset Commit Strategy

Auto-commit is the default and it is wrong for any consumer doing non-trivial processing.

**Checklist:**
- `enable.auto.commit=false` — offsets committed only after the record has been fully processed and downstream writes confirmed
- `max.poll.interval.ms` tuned to the actual processing time for the slowest expected record — not the default 5 minutes. If the team's processing includes a downstream API call with a 30-second timeout, the interval must exceed 30 seconds or the consumer will be evicted from the group mid-retry
- Retry bound declared: the team must state the maximum duration of their retry loop. If a downstream dependency is unavailable for longer than `max.poll.interval.ms`, the consumer will be evicted. This bound must be documented and accepted
- For financial or mission-critical topics: `isolation.level=read_committed` configured — the consumer only processes messages from successfully committed Kafka transactions and never reads uncommitted data from a failed transactional producer

**Why:** a consumer that auto-commits on receipt and then fails during processing has silently lost that record — it will not be redelivered. A consumer with an unbounded retry loop will be evicted by the broker and trigger a rebalance that affects every other member of the group. See `04-Data-Consumption/consumer-groups.md`.

---

## Gate 5 — Lag Monitoring Before Go-Live

The platform team monitors cluster-wide metrics. Individual consumer group lag is the consuming team's responsibility.

**Checklist:**
- Consumer lag alert configured on their consumer group before the first production event is processed — not after they go live
- Alert is on lag **growth rate**, not absolute lag — a stable lag of 10,000 is not an incident; a lag growing at 1,000 messages per minute and not recovering is
- Metrics integrated with the team's existing alerting system (Datadog, PagerDuty, CloudWatch) via the Confluent Cloud Metrics API or JMX
- On-call rotation defined for consumer lag alerts — the platform team does not own consumer group incidents for individual teams

**Why:** a team that discovers their consumer is 6 hours behind because their database was slow has a recovery problem that is their own. A team without lag monitoring discovers this when their product manager asks why data is stale. See `11-Monitoring-Observability/consumer-lag.md`.

---

## Gate 6 — Data Handling and Security

**Checklist:**
- If the topic carries PII or sensitive fields encrypted via CSFLE: the team has requested KMS IAM access for their service principal and confirmed decryption works in staging before production access is granted. Teams without KMS access see ciphertext — they must not attempt to store or process raw encrypted bytes as if they were plaintext
- If the topic is under a data handling policy (GDPR, Australian Privacy Act): the team has acknowledged their obligations in writing — they cannot store topic data in unencrypted logs, cannot replicate to unapproved regions, and must not forward PII fields to third-party systems without explicit consent
- Identity provisioning via Identity Pools where possible: the team's external IdP group membership maps to a Confluent RBAC role (`DeveloperRead`) rather than a manually managed service account. See `09-Security-Architecture/rbac.md` and `09-Security-Architecture/cel-identity-pools.md`

---

## Provisioning — Only After All Gates Pass

Once all gates are confirmed, provisioning is a GitOps PR:

```hcl
# confluent_kafka_acl — consumer group read access
resource "confluent_kafka_acl" "loyalty_consumer_group" {
  kafka_cluster { id = var.cluster_id }
  resource_type = "GROUP"
  resource_name = "cg-loyalty-points-calculator"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.loyalty.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
}

# Topic read access
resource "confluent_kafka_acl" "loyalty_topic_read" {
  kafka_cluster { id = var.cluster_id }
  resource_type = "TOPIC"
  resource_name = "orders.purchase"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.loyalty.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
}

# Quota per principal
resource "confluent_kafka_client_quota" "loyalty_quota" {
  display_name = "loyalty-consumer-quota"
  ingress      = "10485760"   # 10 MB/s
  egress       = "20971520"   # 20 MB/s
  principals   = [confluent_service_account.loyalty.id]
  environment  { id = var.environment_id }
  kafka_cluster { id = var.cluster_id }
}
```

PR review by the platform team is the final gate — a second pair of eyes on principal scope, quota levels, and naming convention compliance. On merge, Terraform applies. The team is live. No tickets, no manual steps. See `10-Operational-Patterns/gitops-terraform.md`.

---

## Cross-References

- Consumer group mechanics and `max.poll.interval.ms` — [04-Data-Consumption/consumer-groups.md](../04-Data-Consumption/consumer-groups.md)
- Static membership for Kubernetes — [04-Data-Consumption/static-membership.md](../04-Data-Consumption/static-membership.md)
- CooperativeStickyAssignor — [04-Data-Consumption/cooperative-rebalancing.md](../04-Data-Consumption/cooperative-rebalancing.md)
- Schema contract and compatibility modes — [08-Stream-Governance/schema-evolution.md](../08-Stream-Governance/schema-evolution.md)
- Data contracts and CEL quality rules — [08-Stream-Governance/data-contracts.md](../08-Stream-Governance/data-contracts.md)
- CSFLE for PII field decryption — [08-Stream-Governance/csfle.md](../08-Stream-Governance/csfle.md)
- RBAC and Identity Pools — [09-Security-Architecture/rbac.md](../09-Security-Architecture/rbac.md)
- Consumer lag monitoring — [11-Monitoring-Observability/consumer-lag.md](../11-Monitoring-Observability/consumer-lag.md)
- GitOps and Terraform provisioning — [10-Operational-Patterns/gitops-terraform.md](gitops-terraform.md)
- Quota management — [13-Performance-Tuning/quota-management.md](../13-Performance-Tuning/quota-management.md)
