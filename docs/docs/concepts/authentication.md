---
title: Authentication
sidebar_position: 3
---

# Authentication

kawa terminates client authentication itself. Clients authenticate to kawa directly via the standard Kafka SASL
handshake; kawa authenticates to the upstream cluster separately as one dedicated service identity. Adding a kawa client
never requires provisioning a matching Kafka principal.

## Protocol

The same `SaslHandshake` + `SaslAuthenticate` flow every Kafka client already implements. No custom client
configuration — point any client at kawa with
`security.protocol=SASL_PLAINTEXT`.

## Configuration

```yaml
auth:
  mechanisms:
    - PLAIN
  clients:
    john:
      password: <encoded-pbkdf2-hash>
    alice:
      password: <encoded-pbkdf2-hash>
```

| Field                      | Type        | Required | Description                                                                             |
|----------------------------|-------------|----------|-----------------------------------------------------------------------------------------|
| `mechanisms`               | string list | yes      | Advertised in `SaslHandshake` responses. Must include every mechanism any client needs. |
| `clients.<name>.password`  | string      | yes      | Encoded credential (PBKDF2-HMAC-SHA256 for PLAIN, SCRAM verifier for SCRAM mechanisms). |
| `clients.<name>.mechanism` | string      | no       | Per-client override. Inherits `mechanisms[0]` when omitted.                             |

### Per-client mechanism override

When a client needs a different mechanism than the global default:

```yaml
auth:
  mechanisms:
    - PLAIN
    - SCRAM-SHA-256
    - SCRAM-SHA-512
  clients:
    john:
      password: <encoded-pbkdf2-hash>  # inherits PLAIN
    alice:
      mechanism: SCRAM-SHA-256         # explicit override
      password: <encoded-credential>
    bob:
      mechanism: SCRAM-SHA-512         # explicit override
      password: <encoded-credential>
```

Every client mechanism must appear in the `mechanisms` list — the gateway advertises this list during handshake, so a
mechanism not listed will be rejected before authentication is even attempted.

SCRAM-SHA-256 and SCRAM-SHA-512 are fully supported for client authentication: kawa runs the real SCRAM
challenge-response exchange (RFC 5802 / RFC 7677) via Kafka's `ScramSaslServer`, so any standard SCRAM client works
with `security.protocol=SASL_PLAINTEXT` and the usual `ScramLoginModule` JAAS configuration.

### Validation

kawa validates auth config at startup:

- A client without `mechanism` + no global `mechanisms` → error
- A client with `mechanism` not in the `mechanisms` list → error
- Blank, missing, or malformed encoded password → error

The admin API accepts a plaintext password when creating or updating a client, hashes it immediately, and persists only
the encoded credential. Client responses never include the password or its encoded hash.

## Upstream broker authentication

kawa can authenticate to the upstream Kafka cluster when the broker requires SASL. Configure `auth.brokerAuth` with the
credentials kawa uses as a client:

```yaml
auth:
  brokerAuth:
    mechanism: PLAIN
    username: kafka
    password: "${KAFKA_PASSWORD}"
```

| Field       | Type   | Required | Description                                |
|-------------|--------|----------|--------------------------------------------|
| `mechanism` | string | yes      | SASL mechanism (`PLAIN` for now)           |
| `username`  | string | yes      | Broker SASL username                       |
| `password`  | string | yes      | Plain-text or `${VAR}` / `${VAR:-default}` |

The gateway authenticates during connection setup — `SaslHandshake` + `SaslAuthenticate`
— before any client traffic is forwarded. This is transparent to clients: they authenticate to the gateway
independently.

:::note Only `PLAIN` is supported for upstream broker authentication. SCRAM would require the `javax.security.sasl.Sasl`
API and is not yet implemented.
:::
