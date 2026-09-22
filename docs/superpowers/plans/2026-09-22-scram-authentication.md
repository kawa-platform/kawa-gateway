# Plan: SCRAM-SHA-256/512 client authentication (end-to-end)

Design: `docs/superpowers/specs/2026-09-22-scram-authentication-design.md` (committed, approved).

Goal: real SCRAM clients (producer/consumer with `ScramLoginModule`) authenticate to kawa end-to-end. Reuse Kafka's
`ScramSaslServer` (kafka-clients 4.3.1) — the exact server real brokers run. Upstream broker auth stays PLAIN.

## File structure

New files:
- `kawa-server/src/main/java/io/jonasg/kawa/server/auth/ScramCredentials.java` — credential bridge (Task 2)
- `kawa-server/src/test/java/io/jonasg/kawa/server/auth/ScramCredentialsTest.java` (Task 2)
- `kawa-integration-tests/src/test/java/io/jonasg/kawa/it/ScramAuthenticationIT.java` (Task 4)
- `kawa-integration-tests/src/test/java/io/jonasg/kawa/it/ScramSaslAuthenticateIT.java` (Task 4)

Modified files:
- `kawa-server/src/main/java/io/jonasg/kawa/server/auth/SaslAuthenticator.java` (Tasks 1, 3)
- `kawa-server/src/main/java/io/jonasg/kawa/server/auth/AuthenticationResult.java` (Task 3)
- `kawa-server/src/main/java/io/jonasg/kawa/server/KafkaClientRequestHandler.java` (Task 1)
- `kawa-server/src/test/java/io/jonasg/kawa/server/auth/SaslAuthenticatorTest.java` (Tasks 1, 3)
- `kawa-server/src/test/java/io/jonasg/kawa/server/DynamicConfigManagerTest.java` (Task 1 — pre-existing dirty file; only the SASL call sites change)
- `kawa-server/src/test/java/io/jonasg/kawa/server/DynamicGatewayStateTest.java` (Task 1)
- `kawa-server/src/test/java/io/jonasg/kawa/server/KafkaClientRequestHandlerTest.java` (Task 1)
- `kawa-integration-tests/src/test/java/io/jonasg/kawa/it/GatewayTestSupport.java` (Task 4)
- `kawa-config/src/main/java/io/jonasg/kawa/config/HashedPassword.java` (Task 5 — Javadoc only)
- `docs/docs/concepts/authentication.md` (Task 5)

Do NOT touch the other pre-existing dirty files (`KafkaApiSpec.java`, `DynamicGatewayState.java`, `KafkaListenerTest.java`).

## Build & test commands

- Unit: `./mvnw -pl kawa-server -am test` (Java 26 active via `.sdkmanrc`; checkstyle runs in `validate`)
- Single class: `./mvnw -pl kawa-server -am test -Dtest=SaslAuthenticatorTest -Dsurefire.failIfNoSpecifiedTests=false`
- Integration (failsafe, Docker required): `./mvnw -pl kawa-integration-tests -am verify -Dit.test=ScramAuthenticationIT,ScramSaslAuthenticateIT`

---

## Task 1 — Session-threaded authenticator API + default mechanisms + ILLEGAL_SASL_STATE

### RED

Update the four test files to the new API. All `handleHandshake`/`handleAuthenticate` calls gain a `ClientSession`
first argument and must be preceded by a successful handshake.

**`SaslAuthenticatorTest`** — add a session helper and rewrite:
- `private ClientSession session()` → `new ClientSession(new EmbeddedChannel())` (imports: `io.netty.channel.embedded.EmbeddedChannel`, `io.jonasg.kawa.server.netty.ClientSession`).
- `handshakeReturnsNoneForSupportedMechanism` (PLAIN): `new SaslAuthenticator()` → `handleHandshake(session(), new SaslHandshakeRequestData().setMechanism("PLAIN"))` → errorCode NONE.
- `handshakeRejectsUnsupportedMechanism` (GSSAPI): errorCode `UNSUPPORTED_SASL_MECHANISM`.
- **New default-mechanism assertions** (fixes the pre-existing broken working-tree state): `new SaslAuthenticator().mechanisms()` contains `["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]` (in that order). The two-arg constructor `(Set.of("PLAIN"), clients)` keeps its exact behavior.
- `authenticateWithValidCredentials` / `authenticateWithInvalidCredentials` / `reloadSwapsCredentials` / `concurrentReload` (lines 69-173): add `session()` + handshake before each `handleAuthenticate`.
- **New test `authenticateWithoutHandshakeReturnsIllegalSaslState`**: `handleAuthenticate(session(), new SaslAuthenticateRequestData().setAuthBytes(...))` → `Failure` with errorCode `ILLEGAL_SASL_STATE`.
- **New test `sessionClosedDropsPerSessionState`**: handshake on session A, `sessionClosed(A)`, then `handleAuthenticate(A, ...)` → `Failure` ILLEGAL_SASL_STATE (state gone). Handshake on session B still works.

**`DynamicConfigManagerTest`** (lines 89-92, pre-existing dirty file — only these lines change):
```java
var session = new ClientSession(new EmbeddedChannel());
var handshake = sasl.handleHandshake(session, new SaslHandshakeRequestData().setMechanism("PLAIN"));
assertThat(handshake.errorCode()).isEqualTo(Errors.NONE.code());
var authenticate = sasl.handleAuthenticate(session, new SaslAuthenticateRequestData()
        .setAuthBytes("\u0000alice\u0000secret".getBytes(StandardCharsets.UTF_8)));
assertThat(authenticate).isInstanceOf(AuthenticationResult.Success.class);
```
(Add the two imports.)

**`DynamicGatewayStateTest`** (lines 58-61):
```java
var session = new ClientSession(new EmbeddedChannel());
assertThat(state.saslAuthenticator().handleHandshake(session,
        new SaslHandshakeRequestData().setMechanism("PLAIN")).errorCode()).isEqualTo(Errors.NONE.code());
assertThat(state.saslAuthenticator().handleAuthenticate(session,
        new SaslAuthenticateRequestData()
                .setAuthBytes("\u0000alice\u0000secret".getBytes(StandardCharsets.UTF_8))))
        .isInstanceOf(AuthenticationResult.Success.class);
```
(Add imports: `SaslHandshakeRequestData`, `Errors`, `EmbeddedChannel`, `ClientSession`.)

**`KafkaClientRequestHandlerTest.answersSaslAuthenticateLocally`** (line 160): the handler now requires a handshake
before `SaslAuthenticate`. Send a `SaslHandshake` request through the handler first:
```java
dispatcher.handleRequest(session, saslHandshakeRequest("PLAIN", (short) 1));
channel.readOutbound(); // discard handshake response
dispatcher.handleRequest(session, saslAuthenticateRequest("\u0000alice\u0000secret", (short) 2));
```
Add a `saslHandshakeRequest(String mechanism, short version)` helper mirroring `saslAuthenticateRequest` (line 233):
`new SaslHandshakeRequestData().setMechanism(mechanism)` with header `ApiKeys.SASL_HANDSHAKE`.

Run `./mvnw -pl kawa-server -am test` — expect compile failures (RED).

### GREEN

**`SaslAuthenticator`**:
- `private static final Set<String> DEFAULT_MECHANISMS = Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");`
- No-arg constructor → `this(DEFAULT_MECHANISMS, Map.of())`. Two-arg constructor unchanged.
- `private final Map<ClientSession, SaslExchange> exchanges = new ConcurrentHashMap<>();`
- `private static final class SaslExchange { final String mechanism; SaslExchange(String mechanism) { ... } }` (server field added in Task 3).
- `public SaslHandshakeResponseData handleHandshake(ClientSession session, SaslHandshakeRequestData request)`:
  - mechanism not in `snapshot.mechanisms()` → `UNSUPPORTED_SASL_MECHANISM` (unchanged).
  - else record `exchanges.put(session, new SaslExchange(request.mechanism()))` and return NONE.
- `public AuthenticationResult handleAuthenticate(ClientSession session, SaslAuthenticateRequestData request)`:
  - `var exchange = exchanges.get(session); if (exchange == null) return AuthenticationResult.failure(Errors.ILLEGAL_SASL_STATE, "SaslAuthenticate received without a successful SaslHandshake");`
  - `if (exchange.mechanism.equals("PLAIN"))` → existing PLAIN path (unchanged logic; on success `exchanges.remove(session)`).
  - else → SCRAM path (Task 3; for now `throw new UnsupportedOperationException` — no, see below).
- `public void sessionClosed(ClientSession session)` → `exchanges.remove(session);`
- `public Set<String> mechanisms()` → `snapshot.mechanisms()` (add if missing).

For Task 1 GREEN, the SCRAM branch can return a generic `Failure` (SASL_AUTHENTICATION_FAILED) — Task 3 replaces it.
This keeps the module compiling and all non-SCRAM tests green.

**`KafkaClientRequestHandler`**:
- Line 124: `saslAuthenticator.handleHandshake(session, requestData)`.
- Line 148: `saslAuthenticator.handleAuthenticate(session, requestData)`.
- Lines 181-183 `sessionClosed`: add `saslAuthenticator.sessionClosed(session);` alongside `fetchSessions.sessionClosed(session)`.
- Metrics: `result instanceof Failure ? "error" : "ok"` (Pending counts as ok — Task 3).

Run `./mvnw -pl kawa-server -am test` — all green. Commit: `feat(server): thread ClientSession through SaslAuthenticator and default to PLAIN+SCRAM mechanisms`.

---

## Task 2 — Credential bridge

### RED

**`ScramCredentialsTest`** (new, `kawa-server/src/test/java/io/jonasg/kawa/server/auth/`):
- `convertsEncodedVerifierToScramCredential`: `HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_256, "secret")` →
  `ScramCredentials.toScramCredential(password)` → assert `iterations() == 4096`, `salt().length == 32`,
  `storedKey().length == 32`, `serverKey().length == 32`.
- `roundTripsSha512`: same for `Mechanism.SCRAM_SHA_512`.
- `rejectsMalformedEncodedValue`: `HashedPassword.fromEncoded("scram-sha256$4096$only-three-parts")` →
  `assertThatThrownBy(...).isInstanceOf(IllegalArgumentException.class)`.

### GREEN

**`ScramCredentials`** (new, package-private class in `io.jonasg.kawa.server.auth`):
```java
final class ScramCredentials {
    private ScramCredentials() {}

    static ScramCredential toScramCredential(HashedPassword password) {
        String[] parts = password.encoded().split("\\$", -1);
        if (parts.length != 5) {
            throw new IllegalArgumentException("Malformed SCRAM verifier: " + password.encoded());
        }
        int iterations = Integer.parseInt(parts[1]);
        byte[] salt = HexFormat.of().parseHex(parts[2]);
        byte[] storedKey = HexFormat.of().parseHex(parts[3]);
        byte[] serverKey = HexFormat.of().parseHex(parts[4]);
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }
}
```
Imports: `org.apache.kafka.common.security.scram.internals.ScramCredential`, `io.jonasg.kawa.config.HashedPassword`,
`java.util.HexFormat`.

Run `./mvnw -pl kawa-server -am test`. Commit: `feat(server): bridge HashedPassword SCRAM verifiers to ScramCredential`.

---

## Task 3 — SCRAM exchange flow

### RED

**`SaslAuthenticatorTest`** — new SCRAM tests (all use the real `ScramSaslClient` from kafka-clients):
- Helper `ScramSaslClient scramClient(ScramMechanism mechanism, String username, String password)` with a callback
  handler: `NameCallback` → `setName(username)`; `PasswordCallback` → `setPassword(password.toCharArray())`;
  `ScramExtensionsCallback` → `throw new UnsupportedCallbackException(callback)` (client tolerates it).
- `scramSha256AuthenticatesEndToEnd`:
  - `var authenticator = new SaslAuthenticator(Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"), Map.of("alice", new ClientConfig("SCRAM-SHA-256", HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_256, "secret"))));`
  - handshake(session, SCRAM-SHA-256) → NONE.
  - `var client = scramClient(ScramMechanism.SCRAM_SHA_256, "alice", "secret");`
  - `var first = authenticator.handleAuthenticate(session, new SaslAuthenticateRequestData().setAuthBytes(client.evaluateChallenge(new byte[0])));`
  - assert `first` is `AuthenticationResult.Pending`, `errorCode() == NONE`, `authBytes()` non-empty.
  - `var second = authenticator.handleAuthenticate(session, new SaslAuthenticateRequestData().setAuthBytes(client.evaluateChallenge(first.response().authBytes())));`
  - assert `second` is `AuthenticationResult.Success`, `username() == "alice"`, `errorCode() == NONE`, `authBytes()` non-empty, `client.isComplete()` true.
- `scramSha512AuthenticatesEndToEnd`: same with `ScramMechanism.SCRAM_SHA_512` / `Mechanism.SCRAM_SHA_512`.
- `scramWrongPasswordFails`: client with password `"wrong"` → first exchange returns `Pending` (server-first is sent
  before the credential check), second exchange returns `Failure` with `errorCode() == SASL_AUTHENTICATION_FAILED`;
  `client.isComplete()` false (server never sent a server-final message).
- `scramUnknownUserFails`: client with username `"mallory"` → second exchange → `Failure` SASL_AUTHENTICATION_FAILED.
- `scramAuthenticateAfterSuccessIsIllegalSaslState`: after a successful exchange, a third `handleAuthenticate` →
  `Failure` ILLEGAL_SASL_STATE (exchange removed on success).
- `sessionClosedDuringExchangeDropsState`: handshake, first exchange → Pending, `sessionClosed(session)`, second
  exchange → `Failure` ILLEGAL_SASL_STATE.
- `plainStillWorksAfterScramChanges`: existing PLAIN tests keep passing (regression).

### GREEN

**`AuthenticationResult`** — add variant:
```java
record Pending(SaslAuthenticateResponseData response) implements AuthenticationResult {
    @Override
    public AuthenticationResult onSuccess(Consumer<String> consumer) {
        return this;
    }
}
```

**`SaslAuthenticator`**:
- `SaslExchange` gains `ScramSaslServer server;` (nullable, lazily created).
- SCRAM branch in `handleAuthenticate`:
  ```java
  var exchange = exchanges.get(session);
  if (exchange == null) return failure(Errors.ILLEGAL_SASL_STATE, "SaslAuthenticate received without a successful SaslHandshake");
  if (exchange.mechanism.equals("PLAIN")) { ...existing path, exchanges.remove(session) on success... }
  return scramExchange(session, exchange, request);
  ```
- `private AuthenticationResult scramExchange(ClientSession session, SaslExchange exchange, SaslAuthenticateRequestData request)`:
  - `byte[] authBytes = request.authBytes(); if (authBytes == null) return failAndRemove(session, "SaslAuthenticate request without auth bytes");`
  - lazily create server: `exchange.server = new ScramSaslServer(ScramMechanism.forMechanismName(exchange.mechanism), Map.of(), new ScramCallbackHandler());` — wrap `NoSuchAlgorithmException` → `failAndRemove`.
  - `try { byte[] challenge = exchange.server.evaluateResponse(authBytes); if (exchange.server.isComplete()) { var result = new AuthenticationResult.Success(exchange.server.getAuthorizationID(), new SaslAuthenticateResponseData().setErrorCode(Errors.NONE.code()).setAuthBytes(challenge)); exchanges.remove(session); return result; } return new AuthenticationResult.Pending(new SaslAuthenticateResponseData().setErrorCode(Errors.NONE.code()).setAuthBytes(challenge)); } catch (SaslException | AuthenticationException e) { return failAndRemove(session, "Invalid username or password"); }`
  - `failAndRemove` → `exchanges.remove(session); return AuthenticationResult.failure(Errors.SASL_AUTHENTICATION_FAILED, "Invalid username or password");` (generic message — no user enumeration).
- `private final class ScramCallbackHandler implements CallbackHandler`:
  - reads the volatile `snapshot` at callback time (fresh credentials after reload).
  - `NameCallback` → `callback.setName(callback.getDefaultName());`
  - `ScramCredentialCallback` → look up `snapshot.clients().get(username)`; if present and mechanism matches, `callback.scramCredential(ScramCredentials.toScramCredential(client.password()))`; unknown user → leave unset (server throws generic SaslException).
  - other callbacks → `throw new UnsupportedCallbackException(callback);`
- `AuthenticationResult.failure(Errors, String)` helper: `new AuthenticationResult.Failure(new SaslAuthenticateResponseData().setErrorCode(code).setErrorMessage(message))` (add if not present).

Imports: `org.apache.kafka.common.security.scram.internals.ScramSaslServer`, `ScramMechanism`,
`org.apache.kafka.common.security.scram.ScramCredentialCallback`, `javax.security.auth.callback.*`,
`org.apache.kafka.common.security.auth.AuthenticationException`, `javax.security.sasl.SaslException`.

Run `./mvnw -pl kawa-server -am test` — all green. Commit: `feat(server): authenticate SCRAM-SHA-256/512 clients via ScramSaslServer`.

---

## Task 4 — Integration tests (acceptance)

The server implementation is complete after Task 3; these ITs prove real clients work end-to-end. Write them, run,
and fix any implementation gaps they surface.

**`GatewayTestSupport`** — add helper:
```java
public static Properties scramSaslProps(String bootstrap, String mechanism, String username, String password) {
    var props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_MECHANISM, mechanism);
    props.put(SaslConfigs.SASL_JAAS_CONFIG,
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username
                    + "\" password=\"" + password + "\";");
    return props;
}
```

**`ScramAuthenticationIT`** (extends `GatewayTestSupport`):
- `authConfig()`: PLAIN for `DEFAULT_PRINCIPAL` + `scram256` (SCRAM-SHA-256, `HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_256, "secret256")`) + `scram512` (SCRAM-SHA-512, "secret512"); `mechanisms` = `["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]`.
- `rbacConfig()`: override to grant READ/WRITE on the test topic to all three principals (mirror the existing PLAIN IT's rbac config).
- `initialTopics()`: the round-trip topic.
- Test `scramSha256ProducerConsumerRoundTrip`: producer with `scramSaslProps(bootstrap, "SCRAM-SHA-256", "scram256", "secret256")` → send → consumer with same props → receive (mirror the existing PLAIN round-trip IT).
- Test `scramSha512ProducerConsumerRoundTrip`: same with SCRAM-SHA-512 / `scram512` / `secret512`.

**`ScramSaslAuthenticateIT`** (raw-socket wire test, mirrors `SaslAuthenticateIT`):
- `authConfig()`: SCRAM-SHA-256 only (`mechanisms = ["SCRAM-SHA-256"]`, one client `alice`).
- `rbacConfig()`: grant alice READ/WRITE on the topic.
- Test `scramExchangeOverRawSocket`:
  - `openSocket()`; `sendHandshake(socket, "SCRAM-SHA-256", version)` → assert errorCode NONE.
  - Drive the exchange with a client-side `ScramSaslClient(ScramMechanism.SCRAM_SHA_256, cbh)`:
    - `sendAuthenticate(socket, client.evaluateChallenge(new byte[0]), version)` → assert errorCode NONE + non-empty authBytes; feed to `client.evaluateChallenge(...)`.
    - second `sendAuthenticate(...)` → assert errorCode NONE + non-empty authBytes; `client.isComplete()` true.
  - `SaslAuthenticateIT`'s `sendAuthenticate` takes a String payload — add an overload taking `byte[]` (or convert).
- Test `scramWrongPasswordOverRawSocket`: client with wrong password → second `sendAuthenticate` → errorCode `SASL_AUTHENTICATION_FAILED`.

Run: `./mvnw -pl kawa-integration-tests -am verify -Dit.test=ScramAuthenticationIT,ScramSaslAuthenticateIT` (Docker running).
Commit: `test(integration): verify SCRAM-SHA-256/512 clients end-to-end`.

---

## Task 5 — Docs

- `docs/docs/concepts/authentication.md`:
  - In the client-auth section (after line 59), add a short paragraph: SCRAM-SHA-256 and SCRAM-SHA-512 are fully
    supported for client authentication — kawa runs the real SCRAM challenge-response exchange (RFC 5802/7677) via
    Kafka's `ScramSaslServer`, so any standard SCRAM client works with `security.protocol=SASL_PLAINTEXT`.
  - The line 95-96 note ("Only PLAIN is supported for upstream broker authentication...") stays — upstream remains PLAIN.
- `kawa-config/src/main/java/io/jonasg/kawa/config/HashedPassword.java` — `verify()` Javadoc: state that for SCRAM
  mechanisms verification is not performed here (the encoded verifier is used by the gateway's SCRAM exchange at
  authentication time); the method returns `false` for SCRAM verifiers.
- `openapi.yaml` (lines 710/724/744) already lists SCRAM-SHA-256/512 — no change.

Verify: `npm run build` in `docs/` (Node ≥ 20). Commit: `docs: document end-to-end SCRAM client authentication`.

---

## Commit order

1. `feat(server): thread ClientSession through SaslAuthenticator and default to PLAIN+SCRAM mechanisms`
2. `feat(server): bridge HashedPassword SCRAM verifiers to ScramCredential`
3. `feat(server): authenticate SCRAM-SHA-256/512 clients via ScramSaslServer`
4. `test(integration): verify SCRAM-SHA-256/512 clients end-to-end`
5. `docs: document end-to-end SCRAM client authentication`

## Risks / notes

- `ScramSaslServer.evaluateResponse` throws `SaslException` (generic) for unknown users and wrong passwords — both map
  to the same generic `Failure` message; no user enumeration.
- The `ScramCallbackHandler` reads the volatile snapshot at callback time, so a `reload` mid-exchange picks up new
  credentials (matches the existing PLAIN reload semantics).
- `KafkaClientRequestHandlerTest.answersSaslAuthenticateLocally` changes behavior (handshake now required) — this is
  the intended Kafka-compatible behavior change.
- Pre-existing dirty files: only `DynamicConfigManagerTest` (SASL call sites) is touched; the rest stay untouched.