# Flink SQL ML_PREDICT() — AI Model Integration in Stream Processing

## What It Is

`ML_PREDICT()` is a Confluent Flink SQL function that calls a remote AI model — an embedding model, an LLM, or any HTTP-accessible inference endpoint — inline within a streaming SQL query. Every record flowing through a Flink job can be sent to an external model and the response written back into the stream without leaving the Flink execution context.

The function is the connective tissue between Kafka-native stream processing and the LLM ecosystem. It removes the need to build and operate a separate embedding service: the Flink job itself handles batching requests to the model API, writing outputs back to the target topic, and recovering correctly from failures.

`ML_PREDICT()` is a Confluent Cloud Flink feature. It is not available in open-source Apache Flink.

---

## Model Connection Setup

API keys and endpoint configuration are kept out of SQL statements via a **model connection** defined once in the Confluent CLI. The connection stores the endpoint URL, API key, and regional routing metadata.

```bash
confluent flink connection create openai-embeddings-connection \
  --cloud aws \
  --region us-west-2 \
  --environment <env-id> \
  --type openai \
  --endpoint "https://api.openai.com/v1/embeddings" \
  --api-key "<secret-key>"
```

**Supported providers:**

| Provider | Type value |
|---|---|
| OpenAI | `openai` |
| Azure OpenAI | `azureopenai` |
| AWS Bedrock | `bedrock` |
| AWS SageMaker | `sagemaker` |
| Google AI / Vertex AI | `googleai` / `vertexai` |
| Azure ML | `azureml` |
| Anthropic | `anthropic` |
| Fireworks AI | `fireworks` |

The connection name is referenced in the `CREATE MODEL` statement. Rotating an API key means updating the connection — no SQL changes, no job redeployment.

**Confluent-managed models (no external connection required):** Confluent also runs a fully-managed catalog of open-weight models (e.g. `microsoft/Phi-3.5-mini-instruct`, `BAAI/bge-large-en-v1.5`) directly, with no API key or `model connection` to provision:

```sql
CREATE MODEL managed_embedding
INPUT  (input STRING)
OUTPUT (vector ARRAY<FLOAT>)
WITH (
  'TASK'             = 'embedding',
  'PROVIDER'         = 'CONFLUENT',
  'CONFLUENT.MODEL'  = 'BAAI/bge-large-en-v1.5'
);
```

Use this when the goal is a quick embedding/classification pipeline without owning API key rotation or an external provider relationship; use a named provider connection when the workload needs a specific frontier model (GPT-4-class, Claude) or existing provider account.

---

## Model Definition

Register the model in Flink SQL before using it in a query. The definition specifies the task type, input/output schema, and which connection to use.

```sql
CREATE MODEL openai_embeddings
INPUT  (input STRING)
OUTPUT (vector ARRAY<FLOAT>)
WITH (
  'TASK'               = 'embedding',
  'PROVIDER'           = 'OPENAI',
  'OPENAI.CONNECTION'  = 'openai-embeddings-connection'
);
```

**Task types:**

| Task | Use |
|---|---|
| `embedding` | Convert text to a vector representation |
| `classification` | Return a label or category |
| `text_generation` | Call an LLM and return generated text |

The `OUTPUT` type must match what the model actually returns. For OpenAI `text-embedding-3-small`, the output is `ARRAY<FLOAT>` with 1536 dimensions. For `text-embedding-3-large`, it is 3072. A mismatch causes a runtime error on first invocation — validate the dimension count from the model provider's documentation before deploying.

---

## Invoking the Model in a Query

`ML_PREDICT()` is invoked via a `LATERAL TABLE` join. Each row of the source stream is passed to the model function, and the result is joined back to the original row.

**Embedding generation:**

```sql
INSERT INTO customer_service_embeddings
SELECT
  log_id,
  log_text,
  vector,
  event_time
FROM customer_service_logs
CROSS JOIN LATERAL TABLE (
  ML_PREDICT('openai_embeddings', log_text)
) AS T(vector);
```

Every record arriving in `customer_service_logs` produces one record in `customer_service_embeddings` with the `vector` field populated.

**LLM inference with prompt construction:**

```sql
INSERT INTO prompts_answer
SELECT
  prompt_id,
  customer_id,
  response
FROM prompts_context
CROSS JOIN LATERAL TABLE (
  ML_PREDICT('gpt4_inference',
    CONCAT('Answer using only the following context:\n', retrieved_context,
           '\n\nQuestion: ', user_query))
) AS T(response);
```

The prompt is constructed inline in SQL. The `CONCAT` builds the augmented prompt from the retrieved context and the original user query.

---

## PII Masking Before Model Calls

Apply masking before the `ML_PREDICT()` call — not after. Once data leaves the platform boundary to an external API endpoint, it is outside your Schema Registry governance perimeter.

Use a Flink UDF or Schema Registry field-level encryption rule to strip or pseudonymise PII fields before the `LATERAL TABLE` join:

```sql
INSERT INTO customer_logs_embeddings
SELECT
  log_id,
  MASK_PII(log_text) AS masked_text,  -- UDF strips card numbers, SSNs
  vector,
  event_time
FROM customer_logs
CROSS JOIN LATERAL TABLE (
  ML_PREDICT('openai_embeddings', MASK_PII(log_text))
) AS T(vector);
```

See `08-Stream-Governance/csfle.md` for field-level encryption and `08-Stream-Governance/pii-tracking.md` for PII tagging strategy.

---

## Schema Registry Integration

Register output schemas for the embeddings topic in Schema Registry under `TopicNameStrategy`. This enforces that:

- The vector dimension count is stable (a dimension change is a schema break)
- Downstream consumers (vector sink connectors) have a guaranteed contract on the output format
- Data lineage tools (Stream Catalog, Stream Lineage) can trace the embedding topic back to its source

Use `FULL_TRANSITIVE` compatibility on embedding topics — consumers reading historical embeddings must be able to process all past versions.

---

## Latency and Throughput Considerations

`ML_PREDICT()` calls are synchronous within the Flink operator: the job sends a request to the model API and waits for the response before writing the output record. The external API call is the dominant latency factor in any job using this function.

| Tradeoff | Faster response | Higher accuracy |
|---|---|---|
| Model | Smaller (e.g., `text-embedding-3-small`) | Larger (e.g., `text-embedding-3-large`) |
| Context window | Shorter prompt | Longer, richer context |
| Flink parallelism | Higher (more concurrent API calls) | Same — parallelism does not affect per-call accuracy |
| Caching | Compacted topic cache bypasses API entirely | No cache — every prompt hits the model |

**Batch API calls:** OpenAI and other providers support batch embedding endpoints that process multiple texts per request. At high throughput, batching reduces API round-trips and cost. Flink's micro-batch processing groups records naturally; tune the Flink checkpoint interval and buffer flush settings to align with the embedding API's batch size limit.

**Rate limiting:** External model APIs impose rate limits (requests per minute, tokens per minute). If Flink's parallelism exceeds the API rate limit, requests fail with 429 errors. Configure Flink's retry policy and back-pressure handling to absorb rate-limit responses without job failure:

```sql
SET 'pipeline.max-parallelism' = '8';  -- cap concurrent API calls
```

Monitor the external API's rate limit headers in application logs. A sustained rate-limit error rate is a signal to either increase API quota or reduce Flink parallelism.

---

## When Not to Use ML_PREDICT()

| Situation | Alternative |
|---|---|
| Model is self-hosted, not HTTP-accessible | Write a Flink UDF that calls the local inference endpoint |
| Embedding is done once at write time and never updated | Run the embedding job offline; load into vector store via batch; no streaming needed |
| Throughput exceeds model API rate limits and cannot be increased | Pre-compute embeddings in a lower-throughput batch window; use streaming for incremental updates only |
| Open-source Flink (not Confluent Cloud) | `ML_PREDICT()` is not available — build a custom Flink Async I/O operator against the model API |

---

## Cross-References

- Streaming RAG four-phase architecture — [streaming-rag.md](streaming-rag.md)
- Flink SQL, state management, and UDFs — [06-Stream-Processing/state-management.md](../06-Stream-Processing/state-management.md)
- PII field-level encryption — [08-Stream-Governance/csfle.md](../08-Stream-Governance/csfle.md)
- Sink connectors for vector stores — [05-Enterprise-Connect/managed-connectors.md](../05-Enterprise-Connect/managed-connectors.md)
- End-to-end worked example — [14-Case-Studies/streaming-rag-airline.md](../14-Case-Studies/streaming-rag-airline.md)
