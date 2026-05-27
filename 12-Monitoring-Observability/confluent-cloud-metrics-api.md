# Confluent Cloud Metrics API — Cloud-Native Observability

## What It Is

The Confluent Cloud Metrics API is a REST API that exposes time-series telemetry for Confluent Cloud resources: Kafka clusters, managed connectors, Schema Registry, Stream Governance components, and Apache Flink compute pools. It is the primary observability mechanism for Confluent Cloud — the equivalent of JMX for self-managed Confluent Platform deployments, but operating at the service level rather than the JVM level.

The API is designed for integration with external monitoring systems (Prometheus, Datadog, Grafana Cloud, New Relic). It does not replace those tools; it provides the data feed they ingest.

## What It Exposes

Metrics are organised by **resource type**:

| Resource | Example metrics |
|---|---|
| Kafka cluster | `io.confluent.kafka.server/received_bytes`, `sent_bytes`, `request_count`, `response_count` |
| Kafka topic | `io.confluent.kafka.server/received_records`, `sent_records` per topic |
| Consumer group | `io.confluent.kafka.server/consumer_lag_offsets` per group/topic/partition |
| Managed connector | `io.confluent.kafka.connect/sent_records`, `failed_record_count`, `connector_status` |
| Flink compute pool | `io.confluent.flink/job_running_count`, `failed_job_count`, `active_task_count` |
| Schema Registry | `io.confluent.kafka.schema_registry/request_count` |

Consumer lag is a first-class metric — available per consumer group, per topic, per partition — without requiring access to broker internals. This is the primary operational advantage over JMX for teams that do not manage the broker infrastructure.

## Authentication

The Metrics API uses HTTP Basic Authentication with a **Cloud API key** scoped to the specific resource being monitored:

```bash
curl -u "$API_KEY:$API_SECRET" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/descriptors/metrics"
```

Access requires the **`MetricsViewer`** RBAC role on the target resource (cluster, connector, or environment). This is a read-only role — it grants no administrative permissions on the resource. Provision dedicated monitoring API keys with `MetricsViewer` only; do not reuse production service account keys.

```bash
# Assign MetricsViewer to a service account for a specific cluster
confluent iam rbac role-binding create \
  --principal User:<service-account-id> \
  --role MetricsViewer \
  --cloud-cluster <cluster-id> \
  --environment <env-id>
```

## Querying the API

**Descriptor endpoint** — list available metrics and their labels:
```bash
GET https://api.telemetry.confluent.cloud/v2/metrics/cloud/descriptors/metrics
```

**Query endpoint** — retrieve time-series data:
```bash
POST https://api.telemetry.confluent.cloud/v2/metrics/cloud/query
Content-Type: application/json

{
  "filter": {
    "op": "AND",
    "filters": [
      { "field": "resource.kafka.id", "op": "EQ", "value": "lkc-abc123" },
      { "field": "metric.label.topic",  "op": "EQ", "value": "orders"   }
    ]
  },
  "granularity": "PT1M",
  "intervals": ["2026-05-27T00:00:00Z/2026-05-27T01:00:00Z"],
  "metric": "io.confluent.kafka.server/consumer_lag_offsets",
  "group_by": ["metric.label.consumer_group_id", "metric.label.partition"]
}
```

Supported granularities: `PT1M` (1 minute), `PT5M`, `PT15M`, `PT1H`. The API retains data at 1-minute resolution for 7 days and at coarser granularities for longer periods.

**Filtering dimensions:**

| Field | Example values |
|---|---|
| `resource.kafka.id` | Cluster ID (`lkc-abc123`) |
| `metric.label.topic` | Topic name |
| `metric.label.consumer_group_id` | Consumer group ID |
| `metric.label.partition` | Partition number |
| `metric.label.principal_id` | Service account or user ID |

## Prometheus Integration via `/export`

The `/export` endpoint returns metrics in **OpenMetrics** format, suitable for scraping by Prometheus:

```bash
GET https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=lkc-abc123
```

Configure Prometheus to scrape this endpoint:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: confluent_cloud
    scrape_interval: 60s
    scheme: https
    basic_auth:
      username: <api-key>
      password: <api-secret>
    static_configs:
      - targets: ['api.telemetry.confluent.cloud']
    metrics_path: /v2/metrics/cloud/export
    params:
      resource.kafka.id: ['lkc-abc123']
```

The scrape interval should be at least 60 seconds — the API updates metrics at 1-minute granularity. Scraping more frequently than once per minute returns the same data and wastes API quota.

## Confluent Cloud vs JMX — Decision Reference

| Dimension | Confluent Cloud Metrics API | JMX (Confluent Platform) |
|---|---|---|
| Access model | REST over HTTPS, no broker access needed | Direct JVM connection or JMX Exporter sidecar on each broker |
| Scope | Cluster, connectors, Flink, Schema Registry in one API | Per-broker MBeans only; no cross-cluster aggregation |
| Consumer lag | First-class metric, per-partition, per-group | Requires consumer group describe or consumer-side JMX |
| Broker internals | Not exposed (Confluent manages the brokers) | Full access: thread pools, log flush, ISR mechanics |
| Operational burden | None — Confluent maintains the telemetry pipeline | Requires JMX Exporter deployment and configuration per broker |
| Data retention | 7 days at 1-minute resolution in the API | Depends on your Prometheus/metrics stack retention |
| Use case | Confluent Cloud clusters — the only option | Confluent Platform self-managed — the primary option |

For hybrid environments (some clusters on Confluent Cloud, others on Confluent Platform), the monitoring stack must handle both: Metrics API for cloud clusters, JMX Exporter for Platform clusters. Unify at the Prometheus/Grafana layer using separate scrape jobs.

## KIP-714 Client-Side Metrics

The Confluent Cloud Metrics API supports **KIP-714**, which enables Kafka client libraries to push client-side telemetry directly to the broker. This surfaces metrics that were previously only available by instrumenting the application itself:

- Producer batch size, record send rate, request latency
- Consumer fetch latency, poll interval
- Connection pool state per broker

KIP-714 is available in Confluent Platform 7.6+ clients and Confluent Cloud. Enable it by setting `telemetry.enabled=true` in the client configuration. Metrics appear in the Metrics API under the `io.confluent.kafka.client` namespace and are visible in Confluent Cloud's built-in dashboards without additional configuration.
