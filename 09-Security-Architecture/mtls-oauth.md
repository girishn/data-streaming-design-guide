# mTLS and OAuth/OIDC — Authentication Flows

## Mutual TLS (mTLS)

Standard TLS authenticates the server to the client — the client verifies the broker's certificate. Mutual TLS adds the reverse: the broker also validates a certificate presented by the client. Both sides prove identity cryptographically before any data is exchanged.

**How it works for Kafka clients:**

1. The client presents a certificate signed by a CA that the broker trusts (configured via `ssl.truststore`)
2. The broker validates the certificate chain up to the trusted CA
3. The broker extracts the client's identity from the certificate's **Subject Common Name (CN)**: `CN=payments-service` becomes the Kafka principal `User:payments-service`
4. ACLs and RBAC rules are applied against this principal

The principal mapping can be customised via `ssl.principal.mapping.rules` on the broker — useful when CN values from an enterprise CA do not match the naming convention your ACLs expect.

**Certificate requirements per client:** in strict mTLS deployments, each service instance has its own unique certificate. This provides non-repudiation — individual instances can be identified in audit logs and revoked independently. The operational cost is certificate issuance, distribution, rotation, and revocation management across every service instance. Certificate management platforms (Vault PKI, cert-manager on Kubernetes) are typically required at scale.

**When mTLS is the right choice:**
- Internal platform-to-platform communication where a CA hierarchy already exists: Connect workers to Schema Registry, brokers to each other (inter-broker), monitoring agents to brokers
- Environments with strict network segmentation where certificate-based identity is the organisational standard
- Regulated industries where mutual authentication and non-repudiation are compliance requirements

## OAuth 2.0 / OIDC

OAuth 2.0 with OIDC provides token-based authentication. Clients authenticate to an external Identity Provider rather than presenting a certificate to the broker directly. The broker validates the token; it does not need to manage a trust store of client certificates.

**Token flow for Kafka clients:**

```
1. Client → IdP:  POST /token  {client_id, client_secret, grant_type=client_credentials}
2. IdP → Client:  JWT access token (signed, short-lived, contains claims)
3. Client → Broker: SASL/OAUTHBEARER with JWT in the SASL exchange
4. Broker → IdP JWKS endpoint: fetch public keys (cached)
5. Broker: validate JWT signature, check expiry (exp claim), check audience (aud claim)
6. Broker: extract principal from configured claim (typically sub or client_id)
```

The broker never sees the client's credentials — only the signed token. The IdP is the authority on identity; the broker is a token validator.

**Token expiry and refresh:** JWTs are short-lived (typically 1 hour or less). Kafka clients must implement a background refresh that obtains a new token before the current one expires. The Confluent Kafka client libraries handle this automatically when configured with a token refresh callback. A token that expires mid-session causes the broker to reject subsequent requests — the client must reconnect with a fresh token.

**Supported IdPs:** Okta, Azure Active Directory, Google Identity, Keycloak, and any OIDC-compliant identity provider that issues JWTs with a discoverable JWKS endpoint.

## mTLS vs OAuth — Decision Factors

| Factor | mTLS | OAuth/OIDC |
|---|---|---|
| Identity source | Certificate signed by a trusted CA | JWT issued by an IdP |
| Credential lifetime | Certificate validity period (months to years) | Token lifetime (minutes to hours) |
| Rotation | Certificate renewal + redistribution per instance | Token refresh handled by client library |
| Operational complexity | High — CA management, cert distribution, revocation | Medium — IdP dependency, token refresh logic |
| Fine-grained claims | No — identity only from CN | Yes — custom claims (groups, scopes, attributes) |
| Zero-trust compatibility | Partial — requires mutual knowledge of CA | Strong — short-lived tokens, claim-based access |
| Best fit | Trusted internal paths, compliance-mandated mutual auth | Diverse external clients, microservices at scale, zero-trust environments |

**Combining both:** mTLS and OAuth are not mutually exclusive. A common production pattern is mTLS for internal broker-to-broker and Connect-to-broker traffic (where certificate management is centralised) and OAuth for application clients (where developer teams manage IdP credentials without needing CA access). Multi-protocol listeners make this combination possible — see `09-Security-Architecture/multi-protocol-auth.md`.
