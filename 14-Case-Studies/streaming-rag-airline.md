# Case Study — Airline Customer Service RAG Pipeline

## Use Case

A large airline's customer service operation handles millions of queries monthly across chat, email, and voice: baggage policy questions, claim submissions, booking changes, flight status, loyalty programme inquiries. Response quality is inconsistent — agents work from static knowledge bases updated quarterly, so answers about a recently changed policy are frequently wrong.

The goal: replace the static knowledge base with a streaming RAG system backed by Confluent Cloud. Every policy document, customer service interaction log, and claim record updates the LLM's context in real time. The LLM reasons over current state, not last quarter's snapshot.

**Key constraints:**
- PII in customer interaction logs (names, passport numbers, payment details) must not leave the trust boundary in plaintext
- Claim validation responses must be auditable — regulators require proof of what information the LLM was given
- Post-processing compliance checks must be independently deployable without touching inference logic
- Latency target: p95 end-to-end response under 8 seconds

---

## Topic Topology

```
# Augmentation
ops.customer-service-logs.raw            # Source: Salesforce + CRM connectors
ops.policy-documents.raw                 # Source: S3 connector (PDF parsed to text)
ops.customer-service-logs.embeddings     # Flink ML_PREDICT() output → Pinecone sink
ops.policy-documents.embeddings          # Flink ML_PREDICT() output → Pinecone sink

# Inference chain
prompts.raw                              # Incoming user queries
prompts.embedding                        # Vectorised queries
prompts.context                          # Query + retrieved context (auditable checkpoint)
prompts.answer                           # Raw LLM responses

# Validation
prompts.validated                        # Responses cleared by all checks
prompts.flagged                          # Failed responses — re-inference or human review
claims.alerts                            # Fraudulent or invalid claim responses for downstream blocking

# Cache
prompts.cache                            # cleanup.policy=compact — previous LLM answers by prompt hash
```

All topics use `TopicNameStrategy`. Embeddings topics use `FULL_TRANSITIVE` compatibility — dimension changes are breaking schema changes.

**Partition count:** `prompts.*` topics are partitioned by `session_id` to preserve per-user conversation ordering. `ops.*` topics are partitioned by `document_id` / `log_id` — no ordering requirement, partitioned for throughput.

---

## Phase 1 — Data Augmentation

Two separate augmentation pipelines run continuously: one for customer service interaction logs (high-volume, real-time), one for policy documents (low-volume, triggered on document update).

**Source connectors:**
- Salesforce Source Connector → `ops.customer-service-logs.raw`
- S3 Source Connector (policy documents, parsed to plain text by a Lambda preprocessor) → `ops.policy-documents.raw`

**PII masking:** A Flink job reads `ops.customer-service-logs.raw` and applies a `MASK_PII()` UDF before calling the embedding model. Passport numbers, payment card numbers, and names are replaced with pseudonymised tokens. The masked text is what gets sent to OpenAI.

**Embedding job:**

```sql
CREATE MODEL openai_embeddings
INPUT  (input STRING)
OUTPUT (vector ARRAY<FLOAT>)
WITH (
  'TASK'              = 'embedding',
  'PROVIDER'          = 'OPENAI',
  'OPENAI.CONNECTION' = 'openai-embeddings-connection'
);

INSERT INTO ops_customer_service_logs_embeddings
SELECT
  log_id,
  masked_text,
  vector,
  event_time
FROM ops_customer_service_logs_masked
CROSS JOIN LATERAL TABLE (
  ML_PREDICT('openai_embeddings', masked_text)
) AS T(vector);
```

**Sink:** Pinecone Sink Connector reads `ops.customer-service-logs.embeddings` and `ops.policy-documents.embeddings`, upserts vectors keyed by `log_id` / `doc_id`. Pinecone namespace separation keeps log embeddings and policy embeddings queryable independently.

---

## Phase 2 — Inference

**Prompt vectorisation:**

```sql
INSERT INTO prompts_embedding
SELECT
  prompt_id,
  session_id,
  user_query,
  vector,
  received_at
FROM prompts_raw
CROSS JOIN LATERAL TABLE (
  ML_PREDICT('openai_embeddings', user_query)
) AS T(vector);
```

**Cache check:** Before retrieval, a Flink job checks `prompts.cache` (compacted topic, keyed by a hash of the prompt embedding). An exact or near-duplicate query that was answered previously returns the cached response directly to `prompts.validated`, bypassing retrieval and LLM inference entirely.

**Context retrieval and enrichment:** A consumer group reads `prompts.embedding`, queries Pinecone for top-5 semantically similar embeddings (searching both log and policy namespaces), and writes the enriched record to `prompts.context`:

```json
{
  "prompt_id": "p-8821",
  "session_id": "sess-4492",
  "user_query": "What is the excess baggage fee for Economy on international routes?",
  "retrieved_context": [
    { "source": "policy-documents", "text": "Economy class international excess baggage: USD 12 per kg...", "score": 0.97 },
    { "source": "customer-service-logs", "text": "Customer asked about excess baggage on LHR-JFK route, agent quoted USD 12/kg", "score": 0.91 }
  ]
}
```

`prompts.context` is the audit topic. Every response can be traced back to the exact context chunks the LLM was given.

**LLM inference:**

```sql
INSERT INTO prompts_answer
SELECT
  prompt_id,
  session_id,
  response,
  finish_reason
FROM prompts_context
CROSS JOIN LATERAL TABLE (
  ML_PREDICT('gpt4_inference',
    CONCAT(
      'You are an airline customer service assistant. Answer ONLY from the context below.\n\n',
      'Context:\n', retrieved_context_text,
      '\n\nCustomer question: ', user_query
    ))
) AS T(response, finish_reason);
```

The system prompt instructs the LLM to answer only from supplied context, not from training knowledge — this is the primary hallucination mitigation at the inference stage.

---

## Phase 3 — Workflow Routing

Most queries (policy questions, fare queries, flight status) are simple retrievals handled by Phase 2. Claim submissions and complaint escalations require multi-step workflows.

**Routing classifier:**

A lightweight intent classifier consumer reads `prompts.raw` and routes:

```
"What is the baggage policy?"     →  prompts.simple   (Phase 2)
"My claim #X was rejected..."     →  prompts.workflow  (Phase 3)
"Book me on the next flight..."   →  prompts.workflow  (Phase 3)
```

**Workflow execution (claims example):**

For a claim query, Flink Table API fills data gaps before LLM inference:
1. Extract the claim reference from the prompt
2. Join against `claims.records` (compacted topic, current claim state)
3. Join against `customer.loyalty-profile` (compacted topic, frequent flyer tier)
4. Construct a fully-enriched prompt: claim details + policy limits + customer tier entitlements

The enriched prompt is injected back into `prompts.context`, then proceeds to Phase 2 inference with complete context.

---

## Phase 4 — Post-Processing

Three independent consumer groups validate `prompts.answer` before any response reaches the end user.

**Business rules validator:**

Reads claim-related responses, checks that any quoted compensation amount is within the `morphir_defined_threshold` for the claim type:

```sql
INSERT INTO prompts_flagged
SELECT prompt_id, response, 'EXCEEDS_POLICY_LIMIT' AS flag_reason
FROM prompts_answer
WHERE claim_type IS NOT NULL
  AND extracted_amount > policy_max_amount;
```

**Identifier validator:**

Checks that any ticket number, booking reference, or baggage tag mentioned in the response exists in the bookings system (via a Flink join against a reference table topic):

```sql
INSERT INTO prompts_flagged
SELECT p.prompt_id, p.response, 'INVALID_BOOKING_REF' AS flag_reason
FROM prompts_answer p
LEFT JOIN booking_reference_table b
  ON p.extracted_booking_ref = b.booking_ref
WHERE p.extracted_booking_ref IS NOT NULL
  AND b.booking_ref IS NULL;
```

**Compliance validator:**

Ensures responses do not contain prohibited language (regulatory constraints on advice-giving, offers of compensation exceeding authority limits). A Flink UDF running a local rules engine performs this check without an external API call.

**Flagged response handling:**

`prompts.flagged` → re-inference consumer re-submits the prompt with additional clarifying context (e.g., "The policy maximum for this claim type is USD X"). If re-inference also fails, the record routes to a human review queue via a Slack sink connector.

`claims.alerts` → a separate fraud-detection consumer reads claims-related flagged responses and publishes to `claims.alerts` for downstream blocking systems.

---

## Key Decisions

**Single vector store, separate namespaces vs. two vector stores:** The team chose a single Pinecone instance with `logs` and `policies` namespaces. This simplifies operations but means a single index scan covers both. If policy retrieval recall degrades due to log embedding dominance (volume imbalance), separate indices become necessary. Monitor Pinecone query score distributions per namespace.

**Compacted topic cache vs. Redis:** Redis adds an operational dependency and a failure mode (cache miss on Redis restart). The compacted Kafka topic approach uses existing infrastructure, survives broker restarts, and is replayable. Trade-off: compacted topic lookup is slightly higher latency than Redis (Kafka consumer poll vs. in-memory GET), acceptable here given the dominant LLM call latency.

**Audit topic retention:** `prompts.context` (the audit checkpoint) is retained for 7 years (regulatory requirement). This topic's volume is modest (one record per inference call) but its retention is long. Use Confluent Cloud tiered storage: `local.retention.ms = 7d`, `retention.ms = 7y`. See `02-Broker-Infrastructure/tiered-storage.md`.

**EOS on validation pipeline:** The validation consumer groups use `processing.guarantee=exactly_once_v2`. A validation record being written twice would result in a duplicate flag, which could incorrectly suppress a valid response. Exactly-once is worth the latency overhead here. See `07-Advanced-Reliability/exactly-once-semantics.md`.

---

## Observability

| Signal | Alert threshold | Meaning |
|---|---|---|
| Consumer lag on `prompts.raw` | > 500 messages | Inference pipeline falling behind query volume |
| Consumer lag on `prompts.answer` | > 200 messages | Post-processing validators not keeping up |
| Rate-limit errors on `ML_PREDICT()` | > 1% of calls | OpenAI API quota reached; reduce Flink parallelism or increase quota |
| Flagged rate on `prompts.flagged` | > 5% of responses | Validation failure spike — review business rules or LLM prompt quality |
| Cache hit rate on `prompts.cache` | < 20% | Cache is undersized or prompt variety is too high for caching to be effective |
| Pinecone query latency p99 | > 200ms | Vector store under load; affects retrieval SLA |

---

## Cross-References

- Streaming RAG lifecycle and anti-patterns — [15-AI-GenAI-Patterns/streaming-rag.md](../15-AI-GenAI-Patterns/streaming-rag.md)
- `ML_PREDICT()` SQL syntax and model connections — [15-AI-GenAI-Patterns/flink-ml-predict.md](../15-AI-GenAI-Patterns/flink-ml-predict.md)
- Tiered storage for long-retention audit topics — [02-Broker-Infrastructure/tiered-storage.md](../02-Broker-Infrastructure/tiered-storage.md)
- PII masking and field-level encryption — [08-Stream-Governance/csfle.md](../08-Stream-Governance/csfle.md)
- Exactly-once semantics for validation pipeline — [07-Advanced-Reliability/exactly-once-semantics.md](../07-Advanced-Reliability/exactly-once-semantics.md)
- Compacted topic design — [02-Broker-Infrastructure/partitioning-strategies.md](../02-Broker-Infrastructure/partitioning-strategies.md)
