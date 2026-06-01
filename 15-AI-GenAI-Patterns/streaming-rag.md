# Streaming RAG — Real-Time Retrieval-Augmented Generation

## What It Is

Retrieval-Augmented Generation (RAG) extends an LLM's knowledge beyond its training data by supplying relevant context at inference time, retrieved from a domain-specific knowledge base. The LLM never needs to be retrained — the knowledge base is what gets updated.

**Streaming RAG** uses a Kafka-based data streaming platform as the knowledge backbone rather than a batch-populated vector database. The distinction matters operationally: in a batch RAG system, the knowledge base is a point-in-time snapshot with a staleness lag proportional to the batch cadence. In streaming RAG, every new event updates the knowledge base within seconds. An LLM querying a streaming RAG system reasons over current state, not last night's state.

The second structural difference: in streaming RAG, **each phase of the RAG pipeline is a decoupled service communicating over Kafka topics**. The team building inference logic does not need to coordinate releases with the team building post-processing validation. Each phase has its own topics as contracts, its own consumer groups, and its own deployment lifecycle.

---

## The Four-Phase Streaming RAG Lifecycle

### Phase 1 — Data Augmentation

**What it does:** Transforms raw proprietary data into vector embeddings and loads them into a vector store for semantic search.

**Topic design:**

```
{domain}.raw                    # Source events — customer logs, tickets, documents
{domain}.embeddings             # Vectorised representations of raw events
```

The raw topic holds the original data from source connectors (S3, Salesforce, MongoDB, JIRA — whatever the domain knowledge lives in). The embeddings topic holds the output of the embedding model — `ARRAY<FLOAT>` alongside the source record metadata.

**Processing:** Flink reads from the raw topic, calls a remote embedding model via [`ML_PREDICT()`](flink-ml-predict.md), and writes the resulting vector to the embeddings topic. A sink connector delivers the embeddings topic to the vector store (Pinecone, Milvus, MongoDB Atlas, Weaviate).

**Schema consideration:** Apply `FULL_TRANSITIVE` compatibility on the embeddings topic. The embedding model output schema must remain stable — a downstream dimension change (e.g., from 1536 to 3072 dimensions) is a breaking change that requires a new topic and backfill.

**PII risk:** Raw topics often contain PII. Apply Schema Registry field-level encryption rules or Flink UDFs to mask sensitive fields before they leave the organisation's trust boundary and reach the embedding API endpoint. See `08-Stream-Governance/csfle.md`.

---

### Phase 2 — Inference

**What it does:** Takes an incoming user prompt, retrieves the most semantically relevant context from the vector store, and constructs the augmented prompt sent to the LLM.

**Topic design:**

```
prompts.raw                     # Raw user query as submitted
prompts.embedding               # Vectorised query (for semantic search)
prompts.context                 # Query + retrieved context combined
prompts.answer                  # Final LLM-generated response
```

This multi-topic design is intentional. Each topic is an observable checkpoint — you can audit what context was retrieved for a given prompt (`prompts.context`) independently from the LLM's response (`prompts.answer`). If a response is wrong, the topology tells you whether the retrieval was wrong or the LLM reasoning was wrong.

**Processing:** Flink reads `prompts.raw`, vectorises the query via `ML_PREDICT()`, calls the vector store to retrieve the top-k similar embeddings, joins the result with the original prompt, and writes the enriched record to `prompts.context`. A separate consumer group submits the enriched prompt to the LLM inference endpoint and writes the response to `prompts.answer`.

**Latency target:** The retrieval + enrichment step typically adds 200–600ms depending on vector store latency. The LLM call itself is the dominant cost (1–10s). Consumer group separation between retrieval and inference allows each stage to be scaled and monitored independently.

---

### Phase 3 — Workflows (Reasoning Agent Routing)

**What it does:** Handles compound queries that cannot be answered by a single vector retrieval. A reasoning agent decomposes the query into steps — retrieve from vector store, query a relational database, call a microservice — and orchestrates the sequence.

**When Phase 3 applies:** A user asking "what is the baggage policy?" is a simple retrieval (Phase 2 is sufficient). A user asking "was my claim submitted correctly and what is the standard for that claim type?" requires multiple lookups across heterogeneous systems — that is a workflow.

**Routing logic:**

```
prompts.raw  →  [classifier consumer]  →  prompts.simple   (Phase 2 path)
                                      →  prompts.workflow  (Phase 3 path)
```

The classifier consumer uses Chain of Thought (CoT) or a lightweight intent classifier to route each prompt. Simple factual queries go directly to Phase 2. Multi-step compound queries enter the workflow path.

**Compacted topic as LLM response cache:**

Before sending any prompt to an LLM, check whether the same question (or a semantically equivalent one) has already been answered:

```
prompts.cache           # cleanup.policy=compact, key = prompt embedding hash
```

A Flink query against the compacted cache topic returns any previous answer for the same prompt hash. If a match exists, return the cached answer directly — no LLM call, no token cost. This is not a standard application-level cache; it is a durable, replayable, Kafka-backed cache that survives node restarts and scales with the consumer group.

**Reference data gaps:** Flink Table API fills data gaps in real time. If a prompt requires a customer's frequent flyer number to complete the context but that field is absent from the prompt record, Flink joins against a reference table topic (e.g., `customer.profile` compacted topic) before sending to the LLM.

---

### Phase 4 — Post-Processing

**What it does:** Validates LLM-generated responses against business rules and compliance logic before they reach the end user. This is the hallucination prevention layer.

**Why it belongs in Kafka:** LLM validation logic is developed and deployed independently from inference. The compliance team writes validation consumers against `prompts.answer` without touching the inference pipeline. If the validation logic changes — new regulatory requirement, updated business rule — they redeploy their consumer group without redeploying the inference chain.

**Topic design:**

```
prompts.answer          # LLM responses awaiting validation
prompts.validated       # Responses that passed all checks
prompts.flagged         # Responses that failed validation — route to alerting or re-inference
```

**Validation pipeline:**

Multiple consumer groups operate in sequence (or in parallel for independent checks):

| Consumer group | Check | Example |
|---|---|---|
| `business-rules-validator` | Business logic | Claim amount ≤ policy maximum |
| `identifier-validator` | Referential integrity | Ticket number exists in bookings system |
| `compliance-validator` | Regulatory | Response does not include advice on prohibited products |
| `hallucination-detector` | Factual consistency | Claimed refund amount matches transaction record |

Failed records route to `prompts.flagged`. Depending on severity, flagged records either trigger a re-inference attempt with additional context or route to a human review queue.

**Frameworks:** Morphir and BPML are commonly used for expressing business rules in a machine-readable, auditable form that both Flink consumers and compliance teams can reason about.

---

## Anti-Pattern Checklist

| Anti-pattern | Signal | Fix |
|---|---|---|
| Batch-populated vector store | Knowledge base is hours or days stale; LLM answers questions about yesterday's state | Replace batch job with streaming augmentation pipeline: source connectors → Flink `ML_PREDICT()` → vector sink |
| Single monolithic RAG service | One service owns ingestion, retrieval, inference, and validation; one bug breaks everything | Decompose into four phases with Kafka topics as contracts between them |
| PII in raw embedding topics | Raw customer data crosses into third-party embedding API without masking | Apply CSFLE or Flink UDF masking before `ML_PREDICT()` call; validate with Schema Rules |
| No observable prompt context topic | Cannot diagnose wrong answers — don't know if retrieval or reasoning is at fault | Maintain `prompts.context` as a separate topic; log what context was retrieved for every answer |
| LLM response cache in external Redis | Cache is ephemeral, not replayable, adds a separate infrastructure dependency | Use a compacted Kafka topic as the cache; survives restarts, observable, no additional infra |
| Validation logic embedded in inference service | Compliance team cannot update rules without coordinating with inference team | Separate consumer groups per validation concern; validate against `prompts.answer` topic independently |
| Embeddings topic with delete cleanup policy | Old embeddings are purged; vector store and topic diverge | Use `cleanup.policy=delete` with long `retention.ms` (match vector store TTL), or compact if the key is the document ID |

---

## Cross-References

- `ML_PREDICT()` function syntax and model connection setup — [flink-ml-predict.md](flink-ml-predict.md)
- Compacted topic design — [02-Broker-Infrastructure/partitioning-strategies.md](../02-Broker-Infrastructure/partitioning-strategies.md)
- Schema Rules and field-level encryption for PII — [08-Stream-Governance/csfle.md](../08-Stream-Governance/csfle.md)
- Flink state management and UDFs — [06-Stream-Processing/state-management.md](../06-Stream-Processing/state-management.md)
- Sink connectors for vector stores — [05-Enterprise-Connect/managed-connectors.md](../05-Enterprise-Connect/managed-connectors.md)
- End-to-end worked example — [14-Case-Studies/streaming-rag-airline.md](../14-Case-Studies/streaming-rag-airline.md)
