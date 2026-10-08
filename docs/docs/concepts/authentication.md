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

## Admin API authentication

Kafka clients and admin API callers are separate: SASL clients (`auth.clients`) authenticate on the Kafka listener,
admin users authenticate on the admin HTTP API. By default (`admin.auth.type: none`) the admin API is open: `none`
authenticates nobody and admits every request as `admin`, so admin users, and whether they are enabled, have no effect
on access under it. With
`admin.auth.type: oauth` kawa acts as its own OAuth 2.0 authorization server. It is **headless**: users log in with
the authorization code flow with PKCE (RFC 7636), and the login page belongs to kawa studio, not to the gateway.

```text
kawa studio                 browser                         kawa
 1. PKCE verifier + state ─▶ GET /oauth/authorize ────────▶ validate client + redirect URI
                            ◀── 302 settings.loginUrl?login_challenge=… ─┘
 2. studio's login page ───▶ POST /oauth/login {login_challenge, username, password}
                            ◀── {redirect_to: "<redirect_uri>?code=…&state=…"}
 3. studio's callback ─────▶ POST /oauth/token grant_type=authorization_code, code, code_verifier
                            ◀── access token + refresh token
```

1. kawa studio sends the browser to `GET /oauth/authorize` with its `client_id`, a registered `redirect_uri`, a PKCE
   `code_challenge` (`S256`) and a `state`. kawa validates the request and redirects to the login UI
   (`settings.loginUrl`) with a signed, 5-minute `login_challenge` that carries it.
2. The login UI posts the challenge with the username and password to `POST /oauth/login`. kawa answers with
   `redirect_to`: the client's redirect URI with a single-use, 60-second authorization code.
3. The client redeems the code with its PKCE `code_verifier` at `POST /oauth/token`. This starts a session that lasts
   `settings.sessionTtl` (default 8 hours) and returns a short-lived access token (`typ: at+jwt`, default 15 minutes)
   and a refresh token (`typ: rt+jwt`, valid until the session ends): ES256-signed JWTs with `iss`/`aud` =
   `settings.issuer`, `sub` = the admin user's id and `preferred_username` =
   their email.
4. The client sends the access token as `Authorization: Bearer <token>`. Requests without a valid token get `401`
   with a `WWW-Authenticate: Bearer` challenge.
5. Before the access token expires, the client exchanges the refresh token for new tokens
   (`grant_type=refresh_token`). The refresh token is rotated, but the session end stays the same.

The password grant is not supported (OAuth 2.1): passwords only ever go to `POST /oauth/login`. Login challenges and
codes are signed JWTs, so any gateway replica can continue a flow another started; each replica refuses a code it has
already redeemed. Because the client only speaks the standard authorization code flow, an external OpenID Connect
provider can take kawa's place later without changes to kawa studio.

`/oauth/authorize`, `/oauth/login`, `/oauth/token`, `/.well-known/jwks.json` (the public signing key),
`/.well-known/oauth-authorization-server` (RFC 8414 metadata) and `/docs` are reachable without a token.

### Admin users

- The **bootstrap user** (`admin.auth.bootstrapUser`) comes from the YAML file, typically with
  `password: ${KAWA_ADMIN_PASSWORD}`. It is a seed, not a permanent account: when the gateway starts and no admin user
  exists yet, it creates the bootstrap user as an ordinary stored admin user. From then on it is listed, edited,
  disabled and deleted like any other, and `bootstrapUser` is ignored (the gateway logs that it was). Changing its
  password in the YAML file later has no effect; change it through the API instead. A deleted bootstrap user stays
  deleted. Gateway replicas starting together on an empty config topic all seed the same admin user: its id is derived
  from its email.
- **Admin users** are created with `POST /admin/users`, get a gateway-assigned UUID, and are read, changed and removed
  with `GET`/`PATCH`/`DELETE /admin/users/{id}`. They are stored in the config topic with their password as a
  PBKDF2-HMAC-SHA256 hash, like `PLAIN` client passwords. The email is the username: stored in lower case, unique
  regardless of case. Each admin user also has an optional `displayName`, an `enabled`
  flag (always `true` on creation) and a `createdAt` set by the gateway, never by the client. `PATCH` keeps every field it
  leaves out, so a user can be disabled or renamed without their password. Responses never contain the password or its
  hash. Tokens carry the admin user's id, so changing their email keeps them signed in. Deleting or disabling an admin
  user revokes their tokens immediately: every request checks that the token's `sub` still exists and is enabled. A
  disabled admin user cannot log in; the login fails like a wrong password.
- The stored admin users are the only way in, so kawa refuses the changes that would lock everyone out, with a `409`
  naming the reason: deleting or disabling your own admin user, and deleting or disabling the last enabled one.

#### Recovering from a lockout

If nobody can sign in any more (a forgotten password, say), start the gateway once with `KAWA_ADMIN_RESET=true`. That
start enables the admin user with the bootstrap user's email and resets its password to `admin.auth.bootstrapUser`'s,
or creates it if it was deleted, and logs a warning. Other admin users are left alone. Unset the variable again before
the next start; without it, a start never changes existing admin users.

Failed logins return `invalid_credentials` whether the username or the password is wrong, and take the same time either
way.

Refresh tokens are stateless JWTs, so kawa keeps no session store. Signing out only discards the tokens in the
client; a leaked refresh token stays usable until its session ends, unless the admin user is deleted (which revokes
all of their tokens immediately). Keep `sessionTtl` as short as is practical.

### Signing key

Tokens are signed with the EC P-256 key in `admin.auth.settings.signingKey` (PEM, PKCS#8 or SEC1, with the public key
embedded). Generate one with:

```shell
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out kawa-admin-signing-key.pem
```

and pass it through the environment, for example `settings.signingKey: ${KAWA_ADMIN_SIGNING_KEY}`. Every gateway replica must
use the same key. Without a configured key kawa generates one at startup and logs a warning: tokens then stop working
after a restart and are only accepted by the replica that issued them.

### Writing an authentication provider

Admin authentication is pluggable. `none` and `oauth` are themselves a plugin (`kawa-http-auth`), registered exactly
the way a third-party provider is, so a provider such as `oidc` needs no change in kawa:

1. Implement `io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory` from `kawa-plugin-api`: `name()` is the value
   of `admin.auth.type` that selects it, `create(context)` returns the `AuthenticationProvider`.
2. In `create`, read `context.settings()` (the `admin.auth.settings` section), optionally use `context.adminUsers()`
   (kawa's admin users) and register unauthenticated endpoints with `context.publicEndpoint(...)`, such as a callback
   or key set. Throw `IllegalArgumentException` for bad settings to stop startup.
3. Register the factory from a `KawaPlugin`: `context.registerAuthenticationProvider(new OidcProviderFactory())`, list
   the plugin in `META-INF/services/io.jonasg.kawa.plugin.KawaPlugin`, and put the JAR in `/opt/kawa/plugins/`.

The provider's `authenticate` is called for every non-public admin request. It returns the caller's principal, or
empty to answer `401`. Exactly one provider is active at a time.
