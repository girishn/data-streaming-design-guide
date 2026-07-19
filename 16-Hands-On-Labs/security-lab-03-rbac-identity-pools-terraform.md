# Security Lab 03 — RBAC + Identity Pools via Terraform, Multi-Tenant Scenario

**Mode:** Hands-on (Cloud) — provisions billable Confluent Cloud resources
**Time:** 2.5–3 hours
**Primary references:** [09-Security-Architecture/rbac.md](../09-Security-Architecture/rbac.md), [09-Security-Architecture/cel-identity-pools.md](../09-Security-Architecture/cel-identity-pools.md), [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md)

## Objective

Build the Terraform-managed RBAC and Identity Pool layer for a three-team multi-tenant platform against a real Confluent Cloud org, using GCP Workload Identity Federation as the credential-free authentication approach — the combination the reference docs identify as sitting "between AWS and Azure for self-service ease" and the natural fit given this project's GCP-first stack.

## Prerequisites

- Confluent Cloud org, `confluent` CLI authenticated
- Terraform ≥ 1.5 with the `confluentinc/confluent` provider configured against your org
- A GCP project where you can create service accounts and configure Workload Identity Federation (or a GKE cluster with Workload Identity enabled, if you want to go as far as running real pods — optional; the Terraform/RBAC portion of this lab does not strictly require a running GKE workload)

## Scenario

Three teams share a Dedicated Confluent Cloud cluster:

- `payments` team: produces to `payments.*`, no read access outside their namespace
- `risk` team: produces to `risk.*`, and needs read-only access to one specific `payments` topic (`payments.transaction.initiated.v1`) for fraud scoring — a declared cross-team read, not a blanket prefix grant
- `platform` team: needs `CloudClusterAdmin` at the cluster level and `MetricsViewer` at the org level for a monitoring service account

## Steps

### 1. Register GCP as an Identity Provider

Follow the GCP Workload Identity Federation setup steps in `cloud-idp-integration.md` — register Google as an Identity Provider in Confluent Cloud once for the cluster.

### 2. Terraform: GCP service accounts + Workload Identity bindings

For each team, create a GCP service account (or reuse one per team-role, e.g. `payments-producer@`) and a Workload Identity binding mapping it to a Kubernetes service account name, following the pattern in `cloud-idp-integration.md`'s GCP section. You do not need a real GKE workload running against this — creating the IAM resources and confirming the binding via `gcloud iam service-accounts get-iam-policy` is sufficient to validate the wiring.

### 3. Terraform: Identity Pools with CEL filters

Write Terraform (or `confluent` CLI, but prefer Terraform per this repo's GitOps convention) creating one Identity Pool per team-role, matching on the GCP service account email per the CEL pattern:

```hcl
resource "confluent_identity_pool" "payments_producer" {
  identity_provider {
    id = confluent_identity_provider.gcp.id
  }
  display_name = "payments-producer-pool"
  filter       = "claims.sub == \"payments-producer@${var.gcp_project}.iam.gserviceaccount.com\""
}
```

Repeat for `risk-producer`, `risk-consumer` (for the cross-team read), and `platform-admin`.

### 4. Terraform: RBAC role bindings

Bind roles per the scenario, using the prefix-pattern and resource-level distinctions from `rbac.md`:

```hcl
# payments producer — prefix binding, own namespace only
resource "confluent_role_binding" "payments_producer_write" {
  principal   = "IdentityPool:${confluent_identity_pool.payments_producer.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=payments.*"
}

# risk consumer — cross-team read, SPECIFIC topic, not a prefix
resource "confluent_role_binding" "risk_reads_payments_transaction" {
  principal   = "IdentityPool:${confluent_identity_pool.risk_consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=payments.transaction.initiated.v1"
}

# platform admin — cluster scope
resource "confluent_role_binding" "platform_cluster_admin" {
  principal   = "IdentityPool:${confluent_identity_pool.platform_admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

# platform monitoring — org scope
resource "confluent_role_binding" "platform_metrics_viewer" {
  principal   = "IdentityPool:${confluent_identity_pool.platform_admin.id}"
  role_name   = "MetricsViewer"
  crn_pattern = data.confluent_organization.main.resource_name
}
```

### 5. Apply and verify with real (or simulated) tokens

Apply the Terraform. If you have a running GKE workload with Workload Identity configured, produce/consume as each team's service account and confirm access matches the scenario exactly — `risk` producer cannot read `payments.*` beyond the one declared topic; `payments` producer cannot read anything at all (write-only). If you don't have a live GKE workload, use `confluent kafka acl list` / the Confluent Cloud console's RBAC view to confirm the bindings resolved as intended, and reason through each principal's effective permission set manually.

### 6. Break the scenario deliberately: attempt an out-of-scope access

Attempt to have the `payments` producer's identity read from `risk.*` — confirm it fails. Attempt to have `risk` consumer read a *different* `payments` topic not explicitly granted (e.g., `payments.refund.initiated.v1`) — confirm it also fails, proving the resource-level (non-prefix) binding is scoped as tightly as intended.

### 7. Teardown

```bash
terraform destroy
```

Delete the GCP service accounts and Workload Identity bindings created outside Terraform, if any.

## Validation

- The cross-team read binding uses a resource-level (specific topic) role binding, not a prefix — if you accidentally used `PREFIXED` or a `risk.` prefix for the payments-read grant, you've over-granted access the scenario explicitly said should be narrow. This is exactly the failure mode `rbac.md`'s "Multi-Tenant Platform Access Model" section warns about.
- Step 6's two negative tests both fail as expected — an RBAC layer that only passes the positive tests hasn't been fully validated.

## What You Should Be Able to Explain Afterward

Why a single Identity Pool with a namespace-scoped CEL filter (the AWS IRSA pattern shown in `cloud-idp-integration.md`) would have let you skip creating a Pool per team-role here, and what you'd trade away — per-service granularity — if you'd used that approach instead of the one-pool-per-role pattern this lab used.
