# Security Lab 04 — Zero-Downtime SASL/SCRAM to OAuth Listener Migration

**Mode:** Hands-on (Local)
**Time:** 2–2.5 hours
**Primary reference:** [09-Security-Architecture/multi-protocol-auth.md](../09-Security-Architecture/multi-protocol-auth.md) ("Zero-Downtime Migration Path" section)

## Objective

Execute the four-step migration path from `multi-protocol-auth.md` against a local Confluent Platform Docker Compose cluster, with a live producer running continuously through the entire migration, and confirm it does not drop or fail to deliver a single message during the listener cutover. This lab is about proving the migration procedure is actually zero-downtime, not just reading that it should be.

## Prerequisites

- Docker + Docker Compose
- A local OIDC-compliant IdP for testing — Keycloak is the standard choice and has a well-documented Docker image; run it in the same Compose stack
- Basic familiarity with Confluent Platform `docker-compose.yml` broker configuration

## Setup

### 1. Stand up a Confluent Platform cluster with SASL/SCRAM only

Configure a single-broker (or 3-broker if you want realistic rolling-restart behavior) Confluent Platform Docker Compose stack with exactly one listener, `SASL_SSL/SCRAM-SHA-512`, matching the "LEGACY" listener in the migration pattern. Create a SCRAM user for a test producer.

### 2. Stand up Keycloak and register a test client

Run Keycloak in the same Compose network. Create a realm, a client for your test producer, and confirm you can obtain a JWT via the client credentials grant against Keycloak's token endpoint before touching the broker config at all — validate the IdP independently first.

### 3. Start a continuous producer against the SCRAM listener

Write a small script (Python or a Java producer) that sends one message per second to a test topic, logging the offset and timestamp of every successful send and every failure, and keep it running in the background for the entire remainder of the lab. This running log is your zero-downtime proof — don't skip it and rely on "it seemed fine."

## Migration Steps

Follow the four steps from `multi-protocol-auth.md` exactly, executing each as a distinct, observable action:

### Step 1 — Add the OAuth listener alongside SCRAM

Edit the broker config to add a second listener (`MODERN://:9093`, `SASL_SSL/OAUTHBEARER`) alongside the existing `LEGACY` listener, update `listener.security.protocol.map` and `advertised.listeners`, and perform a rolling restart (if multi-broker) or a single restart (if single-broker — note in your lab notes that single-broker restarts cannot be truly zero-downtime and explain why the guide's "rolling restart, one at a time" instruction specifically assumes a multi-broker cluster).

Confirm your producer log shows no gap in successful sends across the restart, or — if single-broker — the smallest possible gap, and record its duration.

### Step 2 — Configure OAuth JAAS on the broker

Add the `oauthbearer.sasl.jaas.config` pointing at your Keycloak JWKS endpoint, per the config block in `multi-protocol-auth.md`. Restart again if required by your broker config reload behavior and confirm the producer is still unaffected (it's still authenticating via SCRAM at this point, so this restart should have zero impact on it — that's the point of testing it explicitly).

### Step 3 — Migrate the producer

Start a **second** producer instance configured for `SASL/OAUTHBEARER` against the `MODERN` listener, obtaining its token from Keycloak and implementing (or using a client library's built-in) token refresh. Run it in parallel with the original SCRAM producer for a few minutes, confirming both are delivering successfully to the same topic. Then stop the original SCRAM producer — this is the actual "migrate" moment.

### Step 4 — Decommission the legacy listener

Confirm via broker logs or connection metrics that no clients remain on the `LEGACY` listener (only your now-stopped SCRAM producer was ever using it). Remove the `LEGACY` listener from the broker config and do a final restart.

## Validation

- Merge your producer's send log across the entire migration and confirm zero failed sends, and identify the single largest gap between consecutive successful sends — this should correspond only to your Step 1 restart (and Step 2's, if your broker required one), not to anything in Step 3 or 4, since those steps should be invisible to a running client on the other listener.
- Confirm you can articulate, from the log timestamps, exactly which step (if any) caused the longest client-visible interruption, and why — this is the evidence a design review or postmortem would actually ask for, not just "it worked."
- Confirm the OAuth producer's token refresh actually fired at least once during the parallel-run window in Step 3 if the test ran long enough to cross the token's expiry — if the token lifetime is 1 hour and you only ran the overlap for 5 minutes, note that you did not actually exercise refresh behavior and consider extending the run or shortening the token TTL in Keycloak to force it.

## Teardown

```bash
docker compose down -v
```

## What You Should Be Able to Explain Afterward

Why listener isolation (multiple listeners on one broker, not multiple brokers or a proxy) is what makes this migration possible without a maintenance window — and what would have been different (harder, or impossible without downtime) if the original cluster had been configured with only a single listener that had to be reconfigured in place rather than extended.
