# Module 15 — AI and GenAI Patterns

Using Apache Kafka and Confluent Platform as the backbone for real-time AI and Generative AI applications: streaming RAG architecture, Flink-native model inference, and the operational patterns that make LLM-augmented systems observable and trustworthy.

## Files

- [streaming-rag.md](streaming-rag.md) — Four-phase streaming RAG lifecycle: data augmentation via Flink embeddings, multi-topic inference chain, reasoning agent routing with compacted topic cache, and post-processing validation for hallucination prevention.
- [flink-ml-predict.md](flink-ml-predict.md) — Confluent Flink SQL `ML_PREDICT()` function: model connection setup, CREATE MODEL syntax, LATERAL TABLE join pattern, supported providers (OpenAI, Bedrock, Vertex AI), PII masking before API calls, and rate-limit considerations.
