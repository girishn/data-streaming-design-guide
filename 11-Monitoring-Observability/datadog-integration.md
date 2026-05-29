# Datadog Integration for Confluent Cloud

Confluent Cloud uses a "Bring Your Own Monitoring" model — the Metrics API provides the telemetry feed; Datadog ingests, dashboards, and alerts on it. The two do not overlap in responsibility. Confluent owns the data; Datadog owns the operational experience.

---

## Authorization Setup

The integration requires a service account with the `MetricsViewer` role at the organisation level — not cluster level. This is the most common misconfiguration.

**Step 1 — Create a service account for Datadog:**

```bash
confluent iam service-account create datadog-metrics-reader \
  --description "Datadog Metrics API reader"
```

**Step 2 — Assign MetricsViewer at organisation scope:**

```bash
confluent iam rbac role-binding create \
  --principal User:<service-account-id> \
  --role MetricsViewer \
  --current-org
```

`MetricsViewer` at organisation scope grants read access to the Metrics API across all clusters in the organisation. This is intentional — Datadog needs to enumerate all resources dynamically via the `/discovery` endpoint.

**Step 3 — Create a resource-management API key:**

```bash
confluent api-key create \
  --service-account <service-account-id> \
  --resource cloud
```

**Critical:** the API key must be scoped to `--resource cloud` (resource management), not to a specific Kafka cluster (`--resource <cluster-id>`). A cluster-scoped key authenticates against the Kafka data plane — it cannot call the Metrics API and will return an authentication error. This failure mode is not obvious from the error message.

---

## Ingestion Endpoints

Datadog scrapes two Confluent Cloud endpoints:

**`/export` — OpenMetrics format (primary)**

Returns current metric values for all authorised resources in Prometheus/OpenMetrics format. Datadog's Confluent Cloud integration configures a periodic scrape against this endpoint.

```
GET https://api.telemetry.confluent.cloud/v2/metrics/cloud/export
  ?metric.names[]=io.confluent.kafka.server/consumer_lag_offsets
  &metric.names[]=io.confluent.kafka.server/received_bytes
  &resource.kafka.id=<cluster-id>
Authorization: Basic <base64(api-key:api-secret)>
```

**`/discovery` — Dynamic resource enumeration**

Returns all authorised resources (clusters, connectors, Flink pools) so Datadog does not require hardcoded resource IDs. The Datadog Confluent Cloud integration calls this at startup and on a refresh interval to discover new clusters automatically.

```
GET https://api.telemetry.confluent.cloud/v2/metrics/cloud/descriptors/resources
Authorization: Basic <base64(api-key:api-secret)>
```

---

## Datadog Agent Configuration

The Confluent Cloud integration runs as a Datadog Agent check. Configure it in `conf.d/confluent_cloud.d/conf.yaml`:

```yaml
init_config:

instances:
  - api_key: "<confluent-api-key>"
    api_secret: "<confluent-api-secret>"
    # Resources to monitor — omit to discover all via /discovery endpoint
    kafka_cluster_ids:
      - "<cluster-id-1>"
      - "<cluster-id-2>"
    collect_metrics:
      - io.confluent.kafka.server/consumer_lag_offsets
      - io.confluent.kafka.server/received_bytes
      - io.confluent.kafka.server/sent_bytes
      - io.confluent.kafka.server/request_count
      - io.confluent.kafka.connect/sent_records
      - io.confluent.kafka.connect/failed_record_count
      - io.confluent.flink/job_running_count
      - io.confluent.flink/failed_job_count
    # Scrape interval — Metrics API has 1-minute resolution
    min_collection_interval: 60
```

Credentials should be injected from a secrets manager — not hardcoded in the config file. In Kubernetes, use the Datadog Agent's `ENC[]` syntax to reference secrets from Vault or AWS Secrets Manager.

---

## Metric Namespaces in Datadog

Confluent Cloud metrics appear in Datadog under the `confluent.cloud.*` namespace:

| Confluent Metrics API name | Datadog metric name |
|---|---|
| `io.confluent.kafka.server/consumer_lag_offsets` | `confluent.cloud.kafka.consumer_lag_offsets` |
| `io.confluent.kafka.server/received_bytes` | `confluent.cloud.kafka.received_bytes` |
| `io.confluent.kafka.server/sent_bytes` | `confluent.cloud.kafka.sent_bytes` |
| `io.confluent.kafka.server/received_records` | `confluent.cloud.kafka.received_records` |
| `io.confluent.kafka.connect/sent_records` | `confluent.cloud.kafka.connect.sent_records` |
| `io.confluent.kafka.connect/failed_record_count` | `confluent.cloud.kafka.connect.failed_record_count` |
| `io.confluent.flink/job_running_count` | `confluent.cloud.flink.job_running_count` |

**Useful labels for filtering and grouping:**

| Label | Use |
|---|---|
| `resource.kafka.id` | Filter by cluster |
| `metric.label.topic` | Filter by topic |
| `metric.label.consumer_group_id` | Filter by consumer group |
| `metric.principal_id` | Filter by service account — identify noisy neighbours |
| `resource.connector.id` | Filter by connector |

---

## Alert Thresholds

### Consumer Lag

Consumer lag is the primary operational SLI for streaming pipelines. Alert on growth rate, not absolute value — a stable lag of 50,000 is not an incident; a lag growing at 5,000 per minute and not recovering is.

| Alert | Condition | Severity | Action |
|---|---|---|---|
| Lag growth | `derivative(confluent.cloud.kafka.consumer_lag_offsets)` > 0 sustained 5 min | Warning | Check consumer health, downstream dependency availability |
| Lag spike | `max:confluent.cloud.kafka.consumer_lag_offsets` > SLO threshold per group | Warning | Correlate with deployment events, DLQ depth |
| Lag stalled | Lag non-zero and not decreasing for > 30 min | Critical | Consumer likely stuck — check `max.poll.interval.ms` eviction, dead thread |

Set the SLO threshold per consumer group at onboarding — different groups have different latency SLAs. Do not use a single global lag threshold across all consumer groups.

### Broker and Cluster Health

In Confluent Cloud, broker internals are abstracted. Cluster health is expressed as service-level metrics:

| Alert | Condition | Severity | Action |
|---|---|---|---|
| Offline partitions | `confluent.cloud.kafka.offline_partitions_count` > 0 | **Critical — P1** | Data unavailable; escalate to Confluent support immediately |
| Under-replicated partitions | `confluent.cloud.kafka.under_replicated_partitions` > 0 for > 5 min | Warning | Potential broker issue; monitor for recovery |
| ISR shrink rate | `confluent.cloud.kafka.isr_shrinks_per_sec` > 5 sustained | Warning | Cluster instability; check network and broker health |
| Request rate drop | Sudden drop in `confluent.cloud.kafka.request_count` | Warning | Possible broker unavailability or client connection issue |

### Connector Health

A failed connector is a silent data gap — no events flow, no error reaches consumers. Alert aggressively.

| Alert | Condition | Severity | Action |
|---|---|---|---|
| Connector task failed | `confluent.cloud.kafka.connect.failed_record_count` > 0 | **Critical** | Check DLQ depth, connector logs, source system availability |
| No records flowing | `confluent.cloud.kafka.connect.sent_records` = 0 for > 10 min on an active connector | Warning | Connector may be paused, source system unavailable, or task silently stalled |
| Failed record rate | `confluent.cloud.kafka.connect.failed_record_count` rate spike | Warning | Schema compatibility regression or source data quality issue |

### Flink

| Alert | Condition | Severity | Action |
|---|---|---|---|
| Job not running | `confluent.cloud.flink.job_running_count` drops below expected | Critical | Check Flink job state, checkpoint failures |
| Job failure | `confluent.cloud.flink.failed_job_count` > 0 | Critical | Inspect job logs, state store corruption |
| High backlog | Flink input topic consumer lag growing while job is running | Warning | CFU allocation may need scaling |

---

## Dashboard Structure

Organise the Datadog dashboard in three layers matching operational responsibility:

**Platform layer (platform team)**
- Cluster throughput (bytes in/out) — trend over time
- Partition count — growth rate (topic proliferation signal)
- Under-replicated partitions — cluster health
- ISR shrink rate
- Active connection count per cluster

**Pipeline layer (per domain team)**
- Consumer lag per consumer group — one timeseries per group
- DLQ depth per topic — alert if growing
- Connector sent/failed record rate per connector
- Producer error rate per producer principal

**Business layer (optional, for stakeholder visibility)**
- Events processed per domain per hour
- End-to-end pipeline latency (produce timestamp → consume timestamp via heartbeat)
- Data freshness per topic (time since last event)

---

## Health+ — Confluent-Native Proactive Monitoring

**Health+** is Confluent's built-in proactive monitoring service, available on Dedicated clusters. It analyses cluster behaviour against Confluent's operational best-practice model and generates intelligent alerts before issues become incidents — without requiring Metrics API configuration.

Health+ covers:
- ISR instability patterns that precede partition unavailability
- Consumer lag trends with anomaly detection (not just threshold breaches)
- Broker resource saturation signals
- Misconfigured producer settings (`acks`, `linger.ms`, `batch.size` below recommended levels)

**Health+ vs Datadog — when to use each:**

| Concern | Health+ | Datadog |
|---|---|---|
| Confluent-specific best practice alerts | Yes — built-in, no configuration | Requires manual threshold definition |
| Cross-system correlation (Kafka + app + database) | No | Yes |
| Custom SLO thresholds per consumer group | No | Yes |
| Historical dashboards and trend analysis | Limited | Yes |
| On-call routing and incident management | No | Yes (PagerDuty integration) |

Use Health+ as the first line of cluster-level alerting. Use Datadog for pipeline-level SLO monitoring, cross-system correlation, and on-call routing. The two complement each other — Health+ fires on Confluent-specific patterns; Datadog fires on business-defined thresholds.

---

## Cross-References

- Confluent Cloud Metrics API — endpoints, authentication, query patterns — [11-Monitoring-Observability/confluent-cloud-metrics-api.md](confluent-cloud-metrics-api.md)
- Consumer lag measurement — LEO vs committed offset, LSO — [11-Monitoring-Observability/consumer-lag.md](consumer-lag.md)
- Broker JMX metrics for self-managed clusters — [11-Monitoring-Observability/broker-jmx-metrics.md](broker-jmx-metrics.md)
- MetricsViewer RBAC role — [09-Security-Architecture/rbac.md](../09-Security-Architecture/rbac.md)
- Consumer lag as an onboarding gate — [10-Operational-Patterns/consumer-onboarding.md](../10-Operational-Patterns/consumer-onboarding.md)
- Connector availability alerting — [10-Operational-Patterns/connector-onboarding.md](../10-Operational-Patterns/connector-onboarding.md)
