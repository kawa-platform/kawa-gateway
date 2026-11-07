# AGENTS.md

Kafka Access Gateway (`kawa`): a Netty-based gateway that terminates Kafka client connections and enforces RBAC/ACLs
before forwarding requests to a real Kafka cluster. Maven multi-module, Java 26.

## Build & run

- Build with the repo wrapper: `./mvnw` (failsafe/surefire logs land under
  `target/`).
- **The JVM must be Java 26.** `pom.xml` sets `maven.compiler.release=26`. The repo pins the JDK via `.sdkmanrc`
  (`java=26-tem`) — SDKMAN auto-env loads it on entering the repo (enable `sdkman_auto_env` in `~/.sdkmanrc`). The
  default `JAVA_HOME` is Java 21 and fails with
  `class file version 70.0 ... only recognizes ... up to 65.0`, so if auto-env doesn't apply, set it explicitly:
  `JAVA_HOME=$HOME/.sdkman/candidates/java/26-tem`
- **Do NOT pass `-Pca`** — that flag is specific to the separate `~/dev/bac`
  projects and doesn't exist in this repo.
- **Maven version management:** every version number lives in the parent `pom.xml` - as a property in
  `<properties>`, referenced from `<dependencyManagement>` (dependencies) or `<pluginManagement>` (plugins).
  Module POMs declare dependencies and plugins without `<version>`; internal reactor dependencies use
  `${project.version}`.
- Single module + its upstream deps:
  `./mvnw -pl <module> -am test` (with the Java 26 JDK active via `.sdkmanrc`).
- Single test class:
  `./mvnw -pl <module> -am test -Dtest=ClassName -Dsurefire.failIfNoSpecifiedTests=false`

## Test split (easy to get wrong)

- `*Test` classes in the regular modules run via **surefire** — `./mvnw test`.
- `kawa-integration-tests/src/test/java/.../*IT.java` run via **failsafe**. Surefire is configured with `skipTests=true`
  in that module. Use `verify` and filter with `-Dit.test=ClassName` (not `-Dtest=`) to run a single IT.
- The integration tests use **Testcontainers** (Kafka container) — Docker must be running; they are slow and
  network/docker dependent.
- `kawa-http-admin` has two unit tiers: `RouterTest` (pure plumbing/helper) and the HTTP slice
  tests, one per handler (`PutGovernanceRuleHandler` → `PutGovernanceRuleSliceTest`), plus `ServerSliceTest`, all via
  `AdminHttpSliceTestBase`, that boot a real `AdminHttpServer` on an ephemeral port and assert
  the JSON wire format. Handlers, mappers and services are covered through the slice tests; separate unit tests for
  them are the exception. See `kawa-http-admin/AGENTS.md` for the handler → mapper → service flow.

## Test style

- Test methods use `// given` / `// when` / `// then` comment blocks and AssertJ.
- use `junit` to run tests, `assertj` for assertions, and `mockito` for mocks.
- AssertJ assertions carry a custom fail message stating what did not happen, e.g.
  `.withFailMessage(() -> "Client 'alice' was not removed from group 'producers'")`.
  Prefer the lazy `Supplier<String>` form so the message is only built on failure.
- `GatewayTestSupport` (kawa-integration-tests) is the shared lifecycle; subclasses override `authConfig()`/
  `rbacConfig()`/`initialTopics()`.
- **Always verify asynchronous outcomes with Awaitility, never with a one-shot read.** Kafka admin writes (create/delete
  topics, create partitions, alter configs, create/delete ACLs) complete once the KRaft controller commits them; the
  broker's metadata cache and authorizer catch up later. A read right after the write passes locally and fails
  intermittently on CI. In ITs extending `GatewayTestSupport` use `eventually(() -> assertThat(...))` (30s timeout,
  500ms poll); elsewhere use `Awaitility.await()` directly. The same applies to anything else that settles in the
  background (config reloads, consumer group rebalances, gateway state picked up from the config topic). Negative
  checks (`doesNotContain` after a refused request) may stay immediate.
- Test helpers used by more than one module (custom AssertJ assertions, fixtures, Object Mothers) live in
  `kawa-test-support`, not in a module's `src/test`. Statically import `assertThat` from
  `io.jonasg.kawa.test.KawaAssertions` instead of from AssertJ's `Assertions`; it adds `HttpResponseAssert`
  (`assertThat(response).hasStatusCode(200)` prints the request, headers and body on failure).

## Modules (dependency direction)

The build dependency direction is: `kawa-config` → `kawa-core` and
`kawa-protocol-kafka`; `kawa-virtual-topic` depends on core/config; `kawa-rbac` depends on
core/config/virtual-topic; `kawa-governance` depends on core/config; `kawa-broker` depends on governance (the broker-facing
topic port and its Kafka AdminClient implementation); `kawa-http-admin` depends on
core/config/governance/broker/virtual-topic; `kawa-mcp` depends on http-admin; `kawa-server` assembles the runtime modules; and
`kawa-integration-tests` depends on `kawa-server`. `kawa-test-support` is consumed with `test` scope only; it may
depend on production modules such as `kawa-config` or `kawa-broker` (for `FakeTopicAdmin`), but never on a module that uses it.
`kawa-broker`'s own tests must not use `kawa-test-support`, or the two would form a cycle.

- `kawa-config`: `BootstrapConfig` (the startup YAML, loaded by `ConfigLoader`, which rejects unknown keys and
  dynamic sections) and `DynamicConfig` (the config-topic snapshot: client auth, RBAC, virtual topics, governance,
  admin users, modules, OAuth clients; changed through the admin API), plus `ResourceConfig`/`RbacConfig` (plain Jackson; no
  custom `ResourceType` deserializer).
- `kawa-core`: `GatewayContext`, interceptors.
- `kawa-virtual-topic`: `VirtualTopicInterceptor` + per-API transforms; owns `VirtualTopicManager`
  (the virtual-to-physical mapping state).
- `kawa-protocol-kafka`: `KafkaApiRegistry` — the decoded API/version table; APIs not registered pass through ungated.
- `kawa-rbac`: `AuthorizationInterceptor` + per-API `AuthorizationCheck`s. One check per gate shape; RBAC is
  **on by default** and **default-deny**. It may be explicitly disabled, and doing so removes authorization
  entirely (fail-open: no interceptor, every Kafka request admitted) - see epic #45.
- `kawa-http-admin`: admin HTTP surface (e.g. `GET /topics`) exposing gateway state to a UI; reads
  `VirtualTopicManager`/`MetadataCache`, never talks to the broker.
- `kawa-mcp`: MCP server (official `io.modelcontextprotocol.sdk`) served at `POST /mcp` on the admin port. The
  stateless Streamable HTTP transport (`McpEndpoint`) is a `Router.Handler`, so `/mcp` shares the admin pipeline and
  authentication. It is switched by the runtime `mcp` module (`ModuleId.MCP`); while off, `/mcp` answers `404`. Tools
  call the services in `AdminServices` directly, one package per tool (`listtopics/` holds `ListTopicsMcpTool` and its view;
  types shared by tools, such as `FilterView`, stay in the parent package), and answer with MCP-specific
  views as `structuredContent` plus the same JSON as text; the `McpTool` base class turns failures into tool errors and
  logs each call once at info (user, OAuth client, tool, outcome). A token reaching `/mcp` is either an admin
  token or an `mcp`-audience token of a stored OAuth client, which is accepted only there. MCP clients find OAuth on
  their own: the provider's 401 challenge (`AuthenticationProvider.challenge(AuthenticationRequest)`) on
  `/mcp` points at `/.well-known/oauth-protected-resource/mcp` (resource `<publicUrl>/mcp`), which names the issuer.
  Both are built from `admin.publicUrl`, which the `oauth` provider requires when the admin port binds a wildcard
  address.
- `kawa-plugin-api`: public SPI (`KawaPlugin`, `KawaPluginContext`, admin `AuthenticationProviderFactory`/
  `AuthenticationProvider` with the path-aware `challenge(AuthenticationRequest)`, `AuthenticationProviderContext`
  with `adminUsers()` and `oauthClients()` (`OAuthClientDirectory.find(id)` → `OAuthClient`), `KawaPrincipal(name,
  clientId)`, and the minimal `HttpEndpoint` types) for plugins such as Kawa Pro. Must stay dependency-free and
  small; `kawa-server`'s `PluginLoader` discovers implementations via `ServiceLoader` at startup: built-ins on
  kawa's classpath, external plugins from the plugin directory (`--plugins`, `KAWA_PLUGIN_DIR`, `/opt/kawa/plugins`),
  each in its own `PluginClassLoader` that only shares `io.jonasg.kawa.plugin.*` and `org.slf4j.*` with kawa.
  Anything else an external plugin needs it must ship itself, and nothing in kawa's classpath is visible to it. Every
  new type a plugin must exchange with kawa therefore belongs in `kawa-plugin-api`.
- `kawa-http-auth`: kawa's built-in admin API authentication providers (`none`, `oauth` with Nimbus), shipped as a
  `KawaPlugin` (`BuiltInAuthenticationPlugin`). Depends only on `kawa-plugin-api` - never on `kawa-http-admin` or
  `kawa-config` - so the built-ins use exactly the contract a third-party provider gets. Only `kawa-server` depends on
  it (and `kawa-http-admin` and `kawa-mcp` with `test` scope, for the slice tests). The `oauth` provider's OAuth
  clients are `kawa-studio`, configured in its settings (exact redirect URIs, always resolved first), and the OAuth
  clients stored in the config topic (`oauthClients`), managed through `/admin/oauth-clients`
  (`io.jonasg.kawa.http.oauthclients`, which reserves the studio id) and read via
  `AuthenticationProviderContext.oauthClients()` on every request. Their redirect URIs match exactly, except that an
  `http` loopback URI may give `*` as its whole port (`http://localhost:*/callback`). Audience `admin` gets
  `aud = <issuer>` (the whole admin API), `mcp` gets `aud = <publicUrl>/mcp` (only `POST /mcp`). Tokens carry
  `client_id`, surfaced as `KawaPrincipal.clientId`; disabling or deleting a client, or changing its audience, revokes
  its tokens on the next request. `POST /oauth/login` takes either username + password or, for "continue as" in studio,
  just the `login_challenge` with `Authorization: Bearer <admin access token>` (`LoginEndpoint` verifies it with the admin
  access-token `TokenVerifier`: never an `mcp`-audience or refresh token; the user must still exist and be enabled).
  The 401 challenge is path-aware: `/mcp` has its own protected resource metadata
  (`/.well-known/oauth-protected-resource/mcp`).
  Clients may also identify themselves with an OAuth Client ID Metadata Document (CIMD): a `client_id` that is an
  `https` URL (a managed id never contains `:`). CIMD is switched at runtime by the `mcp-cimd` module (`ModuleId.MCP_CIMD`,
  on unless switched off) and only counts while `mcp` is on: `AuthenticationProviderContext.clientIdMetadataDocumentsEnabled()`
  (backed by `ModuleService`, read from the active config) is asked on every request by `OAuthClients.find` (authorize,
  code redemption), `clientOf` (every request, refresh) and the metadata endpoint, so switching it off stops new sign-ins,
  revokes CIMD tokens and drops `client_id_metadata_document_supported` on the next request. The provider setting
  `clientIdMetadataDocuments` (startup-only) is an optional restriction: empty = any URL, otherwise only those exact URLs.
  `UrlPolicy` is the SSRF guard: `https` only and every resolved address must be public (loopback, private, link-local,
  CGNAT, multicast, reserved, cloud metadata, IPv4-mapped IPv6 are refused); the unpinned DNS-rebinding window is
  documented on `ClientIdMetadataDocuments`. Its test-only loopback variant is selected only by the system property
  `kawa.test.clientIdMetadataLoopback` (integration tests), never by a setting. `ClientIdMetadataDocuments` never follows
  redirects, bounds time (5 s), size (64 KB) and cache (256 entries; 5 min, failures 30 s) and accepts a document only
  if its `client_id` is the URL, `redirect_uris` are valid and `token_endpoint_auth_method` is absent or `none`. CIMD
  clients are `mcp` audience only. For consent, the login challenge carries the client's display name (the document's
  `client_name`, sanitized and capped, else the id) and whether it is registered; the public
  `GET /oauth/login-challenge?login_challenge=…` verifies the challenge and returns `{clientId, clientName, redirectHost,
  audience, registered}` for the login UI.
- `kawa-server`: Netty server, broker clients, shading. Produces the runnable shaded jar
  (`mainClass=io.jonasg.kawa.server.GatewayLauncher`).
- `kawa-integration-tests`: real-client + raw-socket wire tests against a broker via
  `GatewayTestSupport`.
- `kawa-test-support`: shared test helpers (`KawaAssertions`, `HttpResponseAssert`). Its `src/main` is test code
  for other modules, so AssertJ is a compile dependency there.

## Interceptors

- An interceptor is named after the **concern** it owns (`AuthorizationInterceptor`, `VirtualTopicInterceptor`,
  `BrokerEndpointInterceptor`), never after its input or the action it performs.
- It dispatches to one class per protocol API, named `<Api><Concern><Kind>` (`MetadataAuthorizationCheck`,
  `FetchVirtualTopicTransform`, `DescribeClusterBrokerEndpointTransform`), through a `<Concern><Kind>Registry` keyed
  by api key, so the class list shows which APIs the concern covers. Supporting a new API is one new class and one
  registry entry, not a branch inside the interceptor.
- Gateway plumbing that must always run (such as broker endpoint rewriting) is its own concern, not folded into a
  feature interceptor, so switching a feature off cannot break it.

## Code style (repo-specific)

- Javadoc is Markdown `///` (Java 23+/JEP 467). `var` for obvious right-hand types; explicit type otherwise.
- Java source and tests use four-space indentation and no tabs. Checkstyle enforces this, along with a 160-character
  line limit, naming conventions, import hygiene, modifier order, blank-line rules, and newline-at-EOF. The
  configuration lives in `checkstyle.xml` and runs during `validate`.

## Docs

The user documentation lives in the separate `kawa-docs` repository, not in this one. When a change here alters
documented behaviour, update `kawa-docs` as well. Keep its RBAC "What is enforced today" table in sync with
`kawa-rbac`'s `AuthorizationInterceptor` registry.
