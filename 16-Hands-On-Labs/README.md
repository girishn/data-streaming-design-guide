# Module 16 — Hands-On Labs

Practical exercises for a staff-level platform engineer building and operating a production data streaming platform on Kafka Connect (CFK on GKE), Confluent Cloud (Dedicated), and Flink. Every lab is grounded in a specific file elsewhere in this guide — do the linked reading first if the concept is unfamiliar, then use the lab to turn it into a decision or a running system.

Each lab is tagged with a **mode**:

| Mode | What it means |
|---|---|
| **Design** | No infrastructure required. A scenario, a set of constraints, and a worked decision or config you produce on paper. Use these to rehearse the reasoning under interview or design-review conditions. |
| **Hands-on (Local)** | Runs against a local `kind`/`minikube` cluster + Docker. No cloud spend. Used for CFK, Kafka Streams, and protocol-migration exercises where the mechanics don't require a managed cloud control plane. |
| **Hands-on (Cloud)** | Runs against a real GCP project and/or Confluent Cloud org (Dedicated cluster, Flink compute pool). Required where the exercise is specifically about a managed-cloud capability — Confluent Cloud Flink, Identity Pools, RBAC via Terraform against a live org. |

Labs do not have to be done in order. Each states its prerequisites explicitly.

## Flink / Stream Processing Track

- [flink-lab-01-framework-selection-design.md](flink-lab-01-framework-selection-design.md) — **Design.** Given three scenarios, apply the layered decision path in [stream-processing-framework.md](../stream-processing-framework.md) to choose Kafka Streams, Flink, or ksqlDB and justify state backend and checkpoint configuration.
- [flink-lab-02-confluent-cloud-flink-sql.md](flink-lab-02-confluent-cloud-flink-sql.md) — **Hands-on (Cloud).** Provision a Confluent Cloud Flink compute pool against a Dedicated cluster, write Flink SQL statements over a live topic, and wire a `CREATE MODEL` / `ML_PREDICT()` call.
- [flink-lab-03-event-time-windowing.md](flink-lab-03-event-time-windowing.md) — **Hands-on (Cloud).** Implement tumbling, hopping, and session windows in Flink SQL with explicit watermark strategies; compare output against an equivalent Kafka Streams topology run locally.
- [flink-lab-04-eos-checkpoint-tuning-design.md](flink-lab-04-eos-checkpoint-tuning-design.md) — **Design.** Given an RTO target and a duplicate-tolerance constraint, size a Flink checkpoint interval, choose EOS vs at-least-once, and write the resulting job configuration.
- [flink-lab-05-connect-vs-flink-boundary-design.md](flink-lab-05-connect-vs-flink-boundary-design.md) — **Design.** Given three end-to-end pipeline requests touching an external CDC source and/or sink, apply [connect-vs-flink-framework.md](../connect-vs-flink-framework.md) to place each stage on the correct side of the Connect/Flink boundary and justify join type, EOS scope, schema-evolution handling, and multi-team isolation.

## Security Track

- [security-lab-01-mtls-oauth-design.md](security-lab-01-mtls-oauth-design.md) — **Design.** Given a service topology (internal platform components + external microservices + a Connect worker on CFK), decide which paths use mTLS vs OAuth and produce the principal/CN mapping.
- [security-lab-02-cfk-local-mtls-oauth.md](security-lab-02-cfk-local-mtls-oauth.md) — **Hands-on (Local + Cloud).** Deploy CFK on a local `kind` cluster with cert-manager-issued internal mTLS between a Connect worker and Schema Registry, then configure that same worker's external connection to a real Confluent Cloud cluster via SASL/OAUTHBEARER.
- [security-lab-03-rbac-identity-pools-terraform.md](security-lab-03-rbac-identity-pools-terraform.md) — **Hands-on (Cloud).** Build CEL filters and Terraform-managed RBAC role bindings for a multi-tenant platform scenario against a real Confluent Cloud org.
- [security-lab-04-scram-to-oauth-migration.md](security-lab-04-scram-to-oauth-migration.md) — **Hands-on (Local).** Run a zero-downtime SASL/SCRAM → OAuth listener migration against a local Confluent Platform Docker Compose cluster, with a live producer that must not drop a single message during the cutover.

## Suggested Sequence

If you want one linear path rather than picking by gap: `flink-lab-01` → `flink-lab-05` (both design,
build the mental model — framework selection, then the Connect/Flink boundary one layer earlier) →
`security-lab-01` → `security-lab-02` (local, no cloud cost) → `security-lab-04` (local) →
`flink-lab-02` → `flink-lab-03` (cloud Flink) → `security-lab-03` (cloud RBAC) → `flink-lab-04`
(design, closes the loop on EOS you exercised hands-on in lab 3).

## Prerequisites Across All Hands-on Labs

- `confluent` CLI installed and authenticated to a Confluent Cloud org with permission to create a Dedicated cluster and a Flink compute pool (billing implications — Dedicated clusters and Flink compute pools are not free tier; confirm cost with your org before provisioning)
- `kind` or `minikube`, `kubectl`, and `helm` for local Kubernetes labs
- `terraform` ≥ 1.5 with the `confluentinc/confluent` provider
- `docker` and `docker compose`
- Confluent for Kubernetes (CFK) operator manifests — see [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md) for the Terraform-driven deployment pattern this module assumes

## Note on Scope

These labs are new content, not sourced from the project's NotebookLM knowledge base — they are exercises built from the design decisions already documented elsewhere in this repo. Where a lab step requires a specific CLI flag, API version, or console workflow that isn't captured in an existing module file, verify it against current Confluent/GCP documentation before running the step — cloud console UIs and CLI syntax change between releases.
