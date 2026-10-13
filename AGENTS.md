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
- `kawa-http-admin` has two unit tiers: `RouterTest`/`KafkaTopicAdminTest` (pure plumbing/helper) and the HTTP slice
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
core/config/virtual-topic; `kawa-governance` depends on core/config; `kawa-http-admin` depends on
core/config/governance/virtual-topic; `kawa-server` assembles the runtime modules; and
`kawa-integration-tests` depends on `kawa-server`. `kawa-test-support` is consumed with `test` scope only; it may
depend on production modules such as `kawa-config`, but never on a module that uses it.

- `kawa-config`: `GatewayConfig`/`ResourceConfig`/`RbacConfig` + YAML
  `ConfigLoader` (plain Jackson; no custom `ResourceType` deserializer).
- `kawa-core`: `GatewayContext`, interceptors.
- `kawa-virtual-topic`: `VirtualTopicInterceptor` + per-API transforms; owns `VirtualTopicManager`
  (the virtual-to-physical mapping state).
- `kawa-protocol-kafka`: `KafkaApiRegistry` — the decoded API/version table; APIs not registered pass through ungated.
- `kawa-rbac`: `AuthorizationInterceptor` + per-API `AuthorizationCheck`s. One check per gate shape; RBAC is
  **on by default** and **default-deny**. It may be explicitly disabled, and doing so removes authorization
  entirely (fail-open: no interceptor, every Kafka request admitted) - see epic #45.
- `kawa-http-admin`: admin HTTP surface (e.g. `GET /topics`) exposing gateway state to a UI; reads
  `VirtualTopicManager`/`MetadataCache`, never talks to the broker.
- `kawa-plugin-api`: public SPI (`KawaPlugin`, `KawaPluginContext`, admin `AuthenticationProviderFactory` and the
  minimal `HttpEndpoint` types) for plugins such as Kawa Pro. Must stay dependency-free and small; `kawa-server`'s
  `PluginLoader` discovers implementations via `ServiceLoader` at startup: built-ins on kawa's classpath, external
  plugins from the plugin directory (`--plugins`, `KAWA_PLUGIN_DIR`, `/opt/kawa/plugins`), each in its own
  `PluginClassLoader` that only shares `io.jonasg.kawa.plugin.*` and `org.slf4j.*` with kawa. Anything else an
  external plugin needs it must ship itself, and nothing in kawa's classpath is visible to it. Every new type a
  plugin must exchange with kawa therefore belongs in `kawa-plugin-api`.
- `kawa-http-auth`: kawa's built-in admin API authentication providers (`none`, `oauth` with Nimbus), shipped as a
  `KawaPlugin` (`BuiltInAuthenticationPlugin`). Depends only on `kawa-plugin-api` - never on `kawa-http-admin` or
  `kawa-config` - so the built-ins use exactly the contract a third-party provider gets. Only `kawa-server` depends on
  it (and `kawa-http-admin` with `test` scope, for the slice tests).
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

`docs/` is a separate Docusaurus site (Node ≥ 20): `npm run start` (dev),
`npm run build`, `npm run typecheck`. Authoritative concept pages live in
`docs/docs/concepts/` (`architecture.md`, `authentication.md`, `rbac.md`,
`virtual-topics.md`). Keep the "What is enforced today" RBAC table in
`rbac.md` in sync with `kawa-rbac`'s `AuthorizationInterceptor` registry.