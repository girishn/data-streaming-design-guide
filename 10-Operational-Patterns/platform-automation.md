# Platform Automation Model — Self-Service vs Platform Ownership

A mature Kafka platform operates on policy-based self-service: teams get what they need through code submissions, policy validation runs automatically, and the platform team reviews only what genuinely requires human judgment. The alternative — ticket-based provisioning — does not scale past a handful of teams.

Industry benchmarks from platforms at this maturity level: 75% reduction in provisioning tickets, 4x faster time-to-production for new event streams.

---

## The Automation Pyramid

```
        ┌─────────────────────────┐
        │   Platform Team Only    │  Cluster sizing, network topology,
        │   (no delegation)       │  global defaults, incident response
        ├─────────────────────────┤
        │   PR Review Gate        │  New service principal (first onboard),
        │   (human approval)      │  PII topics, quota above tiers, cross-team sharing
        ├─────────────────────────┤
        │   Policy-Automated      │  Standard quota increases, DLQ creation,
        │   (no human approval)   │  consumer group ACLs for existing topics
        ├─────────────────────────┤
        │   Fully Automated       │  Topic creation, ACL derivation from naming,
        │   (zero-touch)          │  schema registration, quota application
        └─────────────────────────┘
```

---

## Layer 1 — Fully Automated (Zero-Touch)

These actions require no human approval. A team submits a PR; the pipeline validates and applies.

### Topic Creation
The naming convention is the specification — see `topic-design-framework.md` Layer 5 for the full pattern and when `{event-type}` belongs in the name versus staying a schema field:

```
{domain}.{entity}.v{N}              # default — event_type is a schema field
{domain}.{entity}.{event-type}.v{N} # only when events are genuinely independent
{domain}.{entity}.state.v{N}        # compacted, current-state topic

payments.transaction.v1             # lifecycle events, one topic
customer.loyalty.earned.v1          # independent event
payments.transaction.state.v1       # compacted state topic
```

The CI/CD pipeline derives from the name:
- **Domain prefix** → owning team's service principal → ACL `WRITE` automatically granted
- **Version suffix** → schema subject name → Schema Registry subject created
- **Domain prefix** → default quota tier applied to the principal
- **`state` in the entity-suffix position** → `cleanup.policy=compact` applied; otherwise `delete`

```yaml
# Example: topic-request PR in git
# teams/payments/topics.yaml
topics:
  - name: payments.transaction.v1
    partitions: 12
    retention_hours: 168
    schema: schemas/payments-transaction.avsc
    owners: payments-platform-team
```

Pipeline steps on PR merge: validate naming → check schema compatibility → apply Terraform → register schema → confirm ACL → close ticket automatically.

### ACL Derivation from Naming Convention
No team requests an ACL manually. ACLs are derived:

```hcl
# Terraform module: topic name → ACL
locals {
  domain = split(".", var.topic_name)[0]   # "payments"
}

resource "confluent_kafka_acl" "producer_write" {
  resource_name = var.topic_name
  principal     = "User:${var.domain_principals[local.domain]}"
  operation     = "WRITE"
  permission    = "ALLOW"
}
```

Cross-domain consumers (e-commerce reading `payments.*`) require an explicit ACL PR — that's the human gate, not the default path.

### Schema Registration via CI/CD
`auto.register.schemas=false` on all producers. The pipeline registers schemas:

```yaml
# CI pipeline step
- name: Schema compatibility check
  run: |
    mvn schema-registry:test-compatibility
    mvn schema-registry:register
```

Compatibility check fails the build before the schema reaches production. No human approval needed for a compatible schema change.

### Quota Application
Each domain has a quota tier (small / medium / large / xlarge) declared in platform config. When a new service principal is created for a domain, the tier's quota is applied automatically from Terraform. Teams do not request quotas for new topics within their domain.

---

## Layer 2 — Policy-Automated (No Human Approval, Policy Gate)

These actions are automated but gated by policy rules evaluated at the API layer — not by a human reviewer.

**Standard quota increase** — a team requests the next tier up via a form or PR. Policy checks: is the request within one tier of current? Has the team been at the current tier for > 30 days? If both pass, applied automatically. If not, escalates to platform team.

**DLQ provisioning** — when a topic is created, the DLQ topic is provisioned automatically alongside it. No separate request needed.

**Consumer group ACL for existing topics** — a team requests read access to a topic they don't own. Policy checks: is the topic PII-tagged? Is the topic in the same environment? If clean, ACL applied automatically and ownership recorded. If PII-tagged, escalates to human gate.

**Connector task scaling** — a connector's task count can be increased within the declared maximum without approval. Beyond the maximum, a platform review is required.

---

## Layer 3 — PR Review Gate (Human Approval)

These require a platform team member to review and approve the PR before Terraform applies.

| Action | Why human review |
|---|---|
| New service principal (first team onboard) | Security review of principal scope, naming, credential management plan |
| PII-tagged topic creation | Data handling agreement must be in place; CSFLE encryption mandate verified |
| Cross-domain topic access | Data governance — producer team must consent; access scope reviewed |
| Quota above platform tiers | Capacity planning impact on shared cluster |
| Compacted topic outside reference data patterns | Retention policy correctness review |
| Schema compatibility mode change | Downstream impact assessment required |
| Connector with non-standard SMT chain | SMT bugs affect every record — human review of transform logic |

The PR is the audit trail. Every access grant, quota change, and schema modification has a git commit with author, timestamp, and review comment.

---

## Layer 4 — Platform Team Only (Not Delegated)

These decisions affect the entire cluster and cannot be delegated to individual teams.

**Cluster sizing and CKU allocation** — throughput capacity, broker count, storage tier. Requires understanding of aggregate load across all teams.

**Network topology** — PrivateLink configuration, VPC peering, transit gateway. Changes affect all teams on the cluster.

**Global defaults** — default retention, default compatibility mode, default quota tiers. Changing a global default is a breaking change for teams relying on it.

**Security architecture** — RBAC role definitions, Identity Pool CEL expressions, TLS certificate rotation. Cross-cutting security decisions cannot be delegated.

**Incident response** — under-replicated partitions, controller failover, broker unavailability. Only the platform team has the access and context to respond.

**Schema Registry cluster management** — subject deletion, compatibility mode override for emergency rollback. These are irreversible operations.

---

## What the Pipeline Looks Like

This is the pyramid tiers mapped onto pipeline stages — see `gitops-terraform.md`'s "CI/CD Pipeline Structure" for the full stage list including `terraform fmt`/`validate` and the `conftest` policy check; this is the condensed view of where automation ends and human approval begins:

```
Team submits PR (topic YAML + schema file)
          ↓
CI: naming convention check (fails fast on non-compliant names)     ─┐
          ↓                                                          │ Fully Automated
CI: schema compatibility check (Schema Registry Maven plugin)        │ (no human touch
          ↓                                                          │ unless a gate
CI: terraform plan + conftest policy check                           │ below fires)
   (partition limits, replication factor, retention, naming —       ─┘
    see opa-policy-enforcement.md)
          ↓
Policy gate: PII-tagged? → route to human review (PR Review Gate tier)
             Standard topic? → auto-approve
          ↓
Merge → terraform apply
          ↓
Topic created + ACLs applied + Schema registered + Quota set
          ↓
Notification to team: topic ready, bootstrap URL, consumer group naming convention
```

Total time for a standard topic: minutes from PR merge. No ticket. No waiting for a platform team member to manually run a script.

---

## Tools

| Layer | Tool |
|---|---|
| Topic/ACL/Schema as code | Confluent Terraform provider (`confluentinc/confluent`) |
| Schema compatibility gate | Schema Registry Maven/Gradle plugin |
| GitOps apply | GitHub Actions / GitLab CI / ArgoCD |
| Policy evaluation | OPA (Open Policy Agent) or custom CI scripts — see [opa-policy-enforcement.md](opa-policy-enforcement.md) |
| Self-service UI (optional) | Confluent Control Center, Conduktor Console, or Backstage plugin |
| Secrets management | AWS SSM + CSI Secrets Provider |

See `10-Operational-Patterns/gitops-terraform.md` for the full Terraform provider HCL and two-pipeline model.

---

## What Automation Does Not Solve

- **Bad event design** — automation provisions what teams request. A poorly designed partition key, an over-partitioned topic, or a schema without quality rules will be provisioned correctly and cause production problems correctly. Design review (human) is still required for first onboarding.
- **Consumer lag incidents** — automation can alert on lag, but recovery (fixing a slow consumer, scaling, reprocessing from DLQ) is a team responsibility.
- **Schema semantic breaks** — CI catches structural incompatibility. Semantic breaks (field silently zeroed, enum values changed) require CEL quality rules authored by the producer team and consumer-driven contract tests in the pipeline.
- **Cost overruns** — quotas prevent cluster saturation but do not prevent a team from provisioning 200 unnecessary topics. Periodic governance review of unused topics is a platform team responsibility.

---

## Cross-References

- GitOps and Terraform two-pipeline model — [10-Operational-Patterns/gitops-terraform.md](gitops-terraform.md)
- `conftest` policy check (partition/RF/retention/naming enforcement) — [10-Operational-Patterns/opa-policy-enforcement.md](opa-policy-enforcement.md)
- Producer onboarding gates — [10-Operational-Patterns/producer-onboarding.md](producer-onboarding.md)
- Consumer onboarding gates — [10-Operational-Patterns/consumer-onboarding.md](consumer-onboarding.md)
- Connector onboarding gates — [10-Operational-Patterns/connector-onboarding.md](connector-onboarding.md)
- RBAC and service principals — [09-Security-Architecture/rbac.md](../09-Security-Architecture/rbac.md)
- Quota management — [13-Performance-Tuning/quota-management.md](../13-Performance-Tuning/quota-management.md)
- Schema CI/CD gate — [08-Stream-Governance/data-contracts.md](../08-Stream-Governance/data-contracts.md)
