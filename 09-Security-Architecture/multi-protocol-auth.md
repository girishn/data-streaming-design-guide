# Multi-Protocol Authentication — Listener Configuration and Migration

## The Migration Problem

Production Kafka clusters accumulate authentication technical debt. A cluster deployed years ago with `SASL/PLAIN` now needs to support OAuth for new microservices while maintaining connectivity for legacy clients that cannot be updated immediately. A cluster using `SASL/SCRAM` needs to add mTLS for internal broker traffic without breaking existing application credentials.

Confluent Platform supports multiple authentication protocols simultaneously through **listener isolation** — each listener has its own network address, security protocol, and SASL mechanism. Clients connect to the listener that matches their authentication method; the broker handles all of them concurrently.

## Listener Architecture

A Kafka broker exposes one or more listeners. Each listener is defined by:
- A **name** (arbitrary, used in configuration references)
- An **address** (host:port)
- A **security protocol** (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`)
- A **SASL mechanism** (if the protocol is SASL-based): `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, `OAUTHBEARER`

**Example: three listeners on one broker**

```properties
# broker.properties

listeners=INTERNAL://0.0.0.0:9091,EXTERNAL://0.0.0.0:9092,MTLS://0.0.0.0:9093

listener.security.protocol.map=\
  INTERNAL:SASL_SSL,\
  EXTERNAL:SASL_SSL,\
  MTLS:SSL

# SASL mechanisms per listener
sasl.mechanism.inter.broker.protocol=OAUTHBEARER
listener.name.internal.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.external.sasl.enabled.mechanisms=OAUTHBEARER,SCRAM-SHA-512

# mTLS listener requires client certificates
listener.name.mtls.ssl.client.auth=required

# Advertised listeners (what clients see)
advertised.listeners=\
  INTERNAL://broker-1.internal:9091,\
  EXTERNAL://broker-1.company.com:9092,\
  MTLS://broker-1.internal:9093
```

Each listener operates independently. A client connecting to port 9092 can use OAuth or SCRAM; a client connecting to port 9093 must present a valid client certificate. The broker does not conflate authentication across listeners.

## Common Multi-Protocol Patterns

**Internal mTLS + External OAuth:**
```
Port 9091 (INTERNAL): SSL/mTLS — broker-to-broker, Connect workers, Schema Registry clients
Port 9092 (EXTERNAL): SASL_SSL/OAUTHBEARER — application microservices
```
Internal platform components use certificate-based identity via an internal CA. External applications authenticate via the enterprise IdP. Network topology (firewall rules, Kubernetes NetworkPolicy) ensures external clients cannot reach the mTLS listener.

**Legacy SCRAM + New OAuth (migration window):**
```
Port 9092 (LEGACY): SASL_SSL/SCRAM-SHA-512 — existing clients during transition
Port 9093 (MODERN): SASL_SSL/OAUTHBEARER — new clients and migrated clients
```
Both listeners are active simultaneously. Teams migrate their clients to the OAuth listener at their own pace; the legacy listener is decommissioned once no clients remain.

**Kerberos (GSSAPI) + OAuth (hybrid enterprise):**
```
Port 9092 (KERB): SASL_SSL/GSSAPI — on-premises services with Kerberos tickets
Port 9093 (CLOUD): SASL_SSL/OAUTHBEARER — cloud-native services with OIDC tokens
```
Common in hybrid environments where on-premises services are Kerberos-integrated but cloud services use an OIDC provider.

## Zero-Downtime Migration Path

Migrating from `SASL/PLAIN` or `SASL/SCRAM` to OAuth without client downtime:

**Step 1 — Add the new listener alongside the existing one:**
Add the OAuth listener to `listeners`, `listener.security.protocol.map`, and `advertised.listeners`. Restart brokers one at a time (rolling restart). Existing clients on the old listener are unaffected.

**Step 2 — Configure the IdP and Identity Pools (Confluent Cloud) or JAAS (Confluent Platform):**
For Confluent Platform, configure the OAuth JAAS settings on the broker:
```properties
listener.name.external.oauthbearer.sasl.jaas.config=\
  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  jwksEndpointUrl="https://idp.company.com/.well-known/jwks.json" \
  expectedAudience="kafka-cluster" ;
```

**Step 3 — Migrate clients incrementally:**
Update client configurations to point to the OAuth listener and configure the token refresh callback. Test each service before migrating the next. The legacy listener absorbs any clients that cannot be migrated immediately.

**Step 4 — Decommission the legacy listener:**
Once no clients remain on the legacy listener (confirm via broker access logs or connection metrics), remove it from the broker configuration and do a final rolling restart.

## Listener Isolation by Network Path

Multi-protocol listeners are most effective when network topology enforces which listener each client class reaches — client-side configuration alone is not sufficient because a misconfigured client could present the wrong credentials to the wrong listener.

**Kubernetes NetworkPolicy:**
```yaml
# Allow application pods to reach only the OAuth listener port
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
spec:
  podSelector:
    matchLabels:
      role: kafka-client
  egress:
  - ports:
    - port: 9092   # OAuth listener only
```

**Firewall rules:** internal mTLS listeners are bound to internal network interfaces only; external OAuth listeners are exposed through a load balancer with TLS termination. Clients from outside the internal network physically cannot reach the mTLS port.

**Confluent Cloud:** network isolation is enforced at the cluster level via Private Link, VPC Peering, and cluster type (Dedicated required for private networking). See `01-Core-Concepts/kafka-vs-confluent.md` for the networking tier model.
