# Flink Lab 02 — Confluent Cloud Flink SQL Against a Dedicated Cluster

**Mode:** Hands-on (Cloud) — provisions billable Confluent Cloud resources
**Time:** 90 minutes
**Primary references:** [06-Stream-Processing/kafka-streams-vs-flink.md](../06-Stream-Processing/kafka-streams-vs-flink.md), [15-AI-GenAI-Patterns/flink-ml-predict.md](../15-AI-GenAI-Patterns/flink-ml-predict.md)

## Objective

Provision a Confluent Cloud Flink compute pool against a Dedicated Kafka cluster, run Flink SQL statements over a live topic, and wire an `ML_PREDICT()` call end to end. This lab exercises the "Confluent Cloud managed Flink" row from the framework decision table — the point is to feel the operational difference the guide describes (no JobManager/TaskManager to provision) versus reading about it.

## Prerequisites

- Confluent Cloud org with permission to create environments, Dedicated clusters, and Flink compute pools
- `confluent` CLI ≥ latest, authenticated (`confluent login`)
- Cost awareness: a Dedicated cluster (minimum 1 CKU) and a Flink compute pool both bill continuously while running — plan to tear down at the end of the lab, not leave it running overnight
- An API key/secret from a provider supported by `ML_PREDICT()` if you want to complete the optional model-inference section (OpenAI, Bedrock, Vertex AI — see the provider table in `flink-ml-predict.md`); skip that section if you don't have one

## Steps

### 1. Provision the environment and Dedicated cluster

```bash
confluent environment create flink-lab-env
confluent environment use <env-id>

confluent kafka cluster create flink-lab-dedicated \
  --cloud gcp \
  --region us-central1 \
  --type dedicated \
  --cku 1
```

Wait for the cluster to report `PROVISIONED` before continuing (`confluent kafka cluster describe <cluster-id>`).

### 2. Create a source topic and produce sample events

```bash
confluent kafka topic create orders --partitions 6

confluent kafka topic produce orders --parse-key --key-delimiter ":" <<'EOF'
cust-1:{"order_id":"o-1001","customer_id":"cust-1","amount":42.50,"event_time":"2026-07-15T10:00:00Z"}
cust-2:{"order_id":"o-1002","customer_id":"cust-2","amount":118.00,"event_time":"2026-07-15T10:00:05Z"}
cust-1:{"order_id":"o-1003","customer_id":"cust-1","amount":9.99,"event_time":"2026-07-15T10:00:12Z"}
EOF
```

Register the value schema in Schema Registry (Avro or JSON Schema — pick one and stay consistent for the rest of the lab; see `08-Stream-Governance/schema-evolution.md` if you need a refresher on compatibility modes before registering).

### 3. Create a Flink compute pool

```bash
confluent flink compute-pool create flink-lab-pool \
  --cloud gcp \
  --region us-central1 \
  --max-cfu 5
```

Note what you did *not* have to do: no JobManager, no TaskManager, no cluster sizing beyond a CFU ceiling. This is the operational contrast to write down for the "Team and Operational Context" layer in `stream-processing-framework.md`.

### 4. Open a Flink SQL shell and run a stateless query

```bash
confluent flink shell --compute-pool <pool-id> --environment <env-id>
```

```sql
SET 'client.statement-name' = 'orders-preview';
SELECT * FROM orders;
```

Confirm you see the three produced records. Note the implicit schema inference — Flink read the topic's registered schema from Schema Registry without you declaring column types.

### 5. Run a stateful windowed aggregation

```sql
CREATE TABLE customer_5min_totals (
  customer_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  total_amount DOUBLE
);

INSERT INTO customer_5min_totals
SELECT
  customer_id,
  window_start,
  window_end,
  SUM(amount) AS total_amount
FROM TABLE(
  TUMBLE(TABLE orders, DESCRIPTOR(`$rowtime`), INTERVAL '5' MINUTES)
)
GROUP BY customer_id, window_start, window_end;
```

Produce a few more events spread across a 10-minute span and confirm the output table shows two separate 5-minute windows for `cust-1`.

### 6. (Optional) Wire ML_PREDICT() for inline enrichment

Follow the `CREATE MODEL` and `LATERAL TABLE` pattern from `15-AI-GenAI-Patterns/flink-ml-predict.md` exactly — create a model connection, register the model, and join it against the `orders` stream to classify order size (`classification` task type) or generate a summary (`text_generation`). Apply the PII-masking guidance from that file if any field could plausibly be considered sensitive, even in this synthetic dataset — treat it as a rehearsal of the habit, not just this lab's data.

### 7. Teardown

```bash
confluent flink compute-pool delete <pool-id>
confluent kafka cluster delete <cluster-id>
confluent environment delete <env-id>
```

Confirm in the Confluent Cloud console that no compute pool or Dedicated cluster remains in a billable state.

## Validation

- The windowed aggregation output correctly separates `cust-1`'s two orders into different 5-minute buckets based on `event_time`, not wall-clock arrival time — if they land in the same bucket incorrectly or the same window regardless of spacing, check whether you're using event-time (`$rowtime` from the registered timestamp) or accidentally defaulting to processing-time.
- You did not write or manage a checkpoint configuration, a JobManager address, or a TaskManager count anywhere in this lab — if you find yourself trying to configure one, you're conflating Confluent Cloud Flink with self-managed Flink.

## What You Should Be Able to Explain Afterward

Concretely, what "Confluent's managed Flink abstracts cluster management entirely" (from `kafka-streams-vs-flink.md`) means in terms of what you as the operator do and don't touch — compared to what CFK on GKE would require you to manage for Kafka Connect in the security labs.
