# Security Lab 02 — CFK on Local Kind: Internal mTLS + External OAuth to Confluent Cloud

**Mode:** Hands-on (Local for CFK) + Hands-on (Cloud for the external OAuth leg)
**Time:** 3–4 hours
**Primary reference:** [09-Security-Architecture/mtls-oauth.md](../09-Security-Architecture/mtls-oauth.md) ("CFK on EKS — Internal mTLS vs External Connection to Confluent Cloud" section — apply it to GKE here), [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md)

## Objective

Build the actual topology designed on paper in `security-lab-01`: deploy CFK on a local `kind` cluster with cert-manager-issued internal mTLS between a Connect worker and Schema Registry, then configure that same Connect worker's external connection to a real Confluent Cloud cluster via SASL/OAUTHBEARER. The point of this lab is to make the two-auth-paths-on-one-pod distinction from the reference doc concrete — you should finish able to point at exactly which config block controls which hop.

## Prerequisites

- `kind`, `kubectl`, `helm`
- cert-manager installed on the kind cluster
- A Confluent Cloud environment + cluster (Basic tier is sufficient for this lab — the external leg only needs SASL/OAUTHBEARER or SASL/PLAIN, not Dedicated-tier mTLS; reuse a cluster from an earlier lab or provision fresh)
- An OIDC-compliant IdP you can register with Confluent Cloud, or use Confluent Cloud's own OAuth support against a test IdP (Keycloak run locally in Docker is a reasonable stand-in if you don't have access to an enterprise IdP — document that substitution explicitly, since the CLI/console steps for registering a real enterprise IdP will differ)

## Steps

### 1. Stand up the local cluster and cert-manager

```bash
kind create cluster --name cfk-lab
helm repo add jetstack https://charts.jetstack.io
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace --set installCRDs=true
```

Create a self-signed `ClusterIssuer` to act as your internal CA — this stands in for the "CA hierarchy already exists" assumption in `mtls-oauth.md`.

### 2. Install the CFK operator

Follow Confluent's CFK Helm chart installation into a `confluent` namespace. Confirm the operator is running before proceeding — this step's exact chart version and values will drift from what's written here, so check current Confluent documentation for the CFK Helm repo and version rather than assuming a specific one.

### 3. Deploy Schema Registry with CFK-managed internal TLS

Deploy a `SchemaRegistry` CFK custom resource configured for internal mTLS, using your cert-manager `ClusterIssuer`. Confirm CFK issues it a certificate with a CN matching its internal service DNS name (`schemaregistry.confluent.svc.cluster.local` or equivalent).

### 4. Deploy the Connect worker with two distinct auth configs

This is the core of the lab. Configure a single `Connect` CFK custom resource with:

- **Internal**: mTLS to Schema Registry, using a cert-manager-issued certificate for the Connect worker itself
- **External**: `SASL/OAUTHBEARER` to your Confluent Cloud cluster, using the pattern from `mtls-oauth.md`:

```yaml
spec:
  kafka:
    bootstrapEndpoint: <your-confluent-cloud-bootstrap>:9092
    authentication:
      type: oauthbearer
      oauthbearer:
        secretRef: connect-oauth-creds
    tls:
      enabled: true
  schemaRegistry:
    endpoint: https://schemaregistry.confluent.svc.cluster.local:8081
    tls:
      # internal mTLS config referencing the cert-manager-issued cert/key
```

Create the `connect-oauth-creds` Kubernetes Secret containing your IdP client credentials or token endpoint config, per your IdP's requirements.

### 5. Verify both paths independently

- Check the Connect worker's logs for a successful Schema Registry connection (confirms internal mTLS) — look for the TLS handshake and the CN it presented.
- Deploy a trivial connector (a `MockSourceConnector` or similar) and confirm it can register a schema with Schema Registry (exercises the mTLS path end to end, not just the handshake) and produce to the real Confluent Cloud topic (exercises the external OAuth path end to end).

### 6. Break each path independently and observe the failure mode

- Temporarily revoke or expire the Connect worker's internal certificate (delete the cert-manager `Certificate` resource) and confirm the Schema Registry connection fails while the Confluent Cloud OAuth connection is unaffected.
- Separately, invalidate the OAuth credentials (wrong client secret in the Secret) and confirm the reverse — Confluent Cloud connectivity fails while internal Schema Registry mTLS is unaffected.

This step is the actual proof that the two auth paths are independent, per the guide's summary table — don't skip it even though it feels like extra work; a working demo where both paths happen to succeed doesn't prove they're decoupled.

## Teardown

```bash
kind delete cluster --name cfk-lab
```

Delete the Confluent Cloud cluster/environment if provisioned solely for this lab.

## Validation

- You can produce the Connect worker pod's two separate credential sources (a `Certificate`/`Secret` for internal mTLS, a separate `Secret` for external OAuth) and explain why they are not the same identity, per `mtls-oauth.md`'s explicit statement that "the internal mTLS identity is not forwarded."
- Your Step 6 breakage test confirms independence — if breaking one path also broke the other, you've misconfigured something (likely pointed both `tls` blocks at the same cert/trust chain).

## What You Should Be Able to Explain Afterward

Where credentials for the external Confluent Cloud connection actually live at runtime (Kubernetes Secret → CFK injection → Connect worker config) and what changes if you were to switch to a credential-free workload identity pattern (GKE Workload Identity) instead — read `09-Security-Architecture/cloud-idp-integration.md`'s GCP Workload Identity Federation section afterward and identify which of today's steps that pattern would eliminate.
