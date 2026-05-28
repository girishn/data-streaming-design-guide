# Module 14 — Case Studies

End-to-end architecture walkthroughs showing how the patterns across modules 01–13 combine to solve specific production problems.

## Files

- [real-time-shipment-tracking.md](real-time-shipment-tracking.md) — B2B logistics platform replacing batch ETL with event-driven real-time shipment tracking: domain event design, Flink enrichment, CEP for proactive alerting, and the cross-cutting architectural decisions that make it work at scale.
- [financial-ecommerce-eos.md](financial-ecommerce-eos.md) — Exactly-once payment ledger for e-commerce: transactional outbox as the entry point, transactional producer design across the payment lifecycle, read_committed ledger consumers, cross-system idempotency, and regulatory audit trail requirements.
- [banking-platform-design.md](banking-platform-design.md) — Full framework run for an enterprise banking streaming platform on Confluent Cloud: requirement extraction, nine-dimension filter pass, platform emergence from scale requirements, topic tier model, and resolved open decisions for auth and processing framework.
- [retail-platform-design.md](retail-platform-design.md) — Enterprise retail streaming platform for a national Australian retailer: framework validation of a real-world platform spec, private networking and self-managed Connect for on-premises ERP/WMS, CSFLE with Privacy Act erasure gap analysis, GitOps self-service model, and five concrete observations and recommendations.
