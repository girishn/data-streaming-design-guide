# Security Lab 01 — mTLS vs OAuth Design Exercise

**Mode:** Design (no infrastructure)
**Time:** 45–60 minutes
**Primary reference:** [09-Security-Architecture/mtls-oauth.md](../09-Security-Architecture/mtls-oauth.md), [09-Security-Architecture/multi-protocol-auth.md](../09-Security-Architecture/multi-protocol-auth.md)

## Objective

Given a realistic service topology, decide which paths use mTLS and which use OAuth/OIDC, produce the principal/CN mapping, and sketch the listener configuration that enforces the split. This is the design pass to do before touching infrastructure — `security-lab-02` builds the local hands-on version of the CFK portion of this same topology.

## Scenario

A platform runs Confluent for Kubernetes (CFK) on a GKE cluster. The following components exist:

- A Kafka Connect worker running JDBC source connectors, deployed by CFK in the `kafka-connect` namespace
- Schema Registry, also CFK-managed, in the same namespace
- Three application microservices owned by three different teams (`orders-service`, `inventory-service`, `notifications-service`), each running as its own Deployment, each needing to produce and/or consume from specific topic namespaces
- A monitoring stack (New Relic agent) that needs read access to Confluent Cloud Metrics API and to consume from an internal `_confluent-monitoring` topic
- The cluster also connects outward to a Confluent Cloud **Dedicated** cluster for the actual Kafka brokers — CFK here is running Connect and Schema Registry only, not brokers

## Task

### Step 1 — Classify every path

For each of the following six paths, decide mTLS or OAuth/OIDC, and justify using the decision factors table in `mtls-oauth.md`:

1. Connect worker ↔ Schema Registry (both CFK-managed, same namespace)
2. Connect worker → Confluent Cloud Dedicated cluster (external)
3. `orders-service` → Confluent Cloud Dedicated cluster
4. `inventory-service` → Confluent Cloud Dedicated cluster
5. `notifications-service` → Confluent Cloud Dedicated cluster
6. New Relic monitoring agent → Confluent Cloud Metrics API + `_confluent-monitoring` topic

For any path connecting to Confluent Cloud, first check which cluster tier — Dedicated — is in play, and confirm from the "Confluent Cloud vs Self-Managed" table in `mtls-oauth.md` whether mTLS client authentication is even available on that path before deciding.

### Step 2 — Principal / CN mapping

For every path you classified as mTLS, write the certificate CN and the resulting Kafka principal it maps to (e.g., `CN=connect.confluent.svc.cluster.local` → `User:connect`). For every path classified as OAuth, write the claim you'd use to derive the principal or Identity Pool match (reference the CEL filter patterns in `cel-identity-pools.md` if you want to go a layer deeper, though that's the focus of `security-lab-03`, not this one).

### Step 3 — Listener configuration sketch

Using the multi-listener pattern from `multi-protocol-auth.md`, sketch (in `broker.properties`-style pseudo-config, or as a diagram) how many listeners this topology needs and what protocol each one carries. You do not need real broker config since the actual brokers are Confluent Cloud-managed — instead, sketch the **CFK-internal** listener configuration for the Connect-worker-to-Schema-Registry path, and separately state what SASL mechanism the external-facing paths use.

### Step 4 — Network enforcement

Name the specific Kubernetes construct (from `multi-protocol-auth.md`'s "Listener Isolation by Network Path" section) that would prevent `orders-service` from ever being able to reach the internal CFK mTLS listener meant only for Connect ↔ Schema Registry traffic.

## Validation

- Paths 2–5 should all resolve to OAuth/OIDC or SASL/PLAIN, not mTLS — Confluent Cloud Dedicated *can* support Certificate Identity Pools, but the scenario doesn't establish that this platform has set that up, and the guide's default recommendation for application microservices even where available is still OAuth. If you chose mTLS for any of paths 2–5, justify explicitly why you're invoking the Dedicated-tier exception rather than defaulting to it.
- Path 1 should be mTLS — this is exactly the "internal platform-to-platform communication where a CA hierarchy already exists" case the guide names directly.
- Path 6 (monitoring) should map to an `MetricsViewer` RBAC role via a service account or Identity Pool, using OAuth — not mTLS, since Metrics API access is inherently an external HTTPS API call, not a Kafka client connection.

## What You Should Be Able to Explain Afterward

Why "the internal mTLS identity is not forwarded" when a CFK component connects outward to Confluent Cloud — i.e., why you cannot treat the Connect worker's internal certificate identity as if it carries through to its external Confluent Cloud session, and what that implies for anyone assuming CN-based audit trails span both hops.
