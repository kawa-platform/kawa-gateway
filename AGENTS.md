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
- `kawa-http-admin` has two unit tiers: `RouterTest`/`KafkaTopicAdminTest`
  (pure plumbing/helper) and the HTTP slice tests (`TopicSliceTest`,
  `ClientSliceTest`, `RoleSliceTest`, `GroupSliceTest`, `GovernanceSliceTest`,
  `ServerSliceTest` via `AdminHttpSliceTestBase`) that boot a real
  `AdminHttpServer` on an ephemeral port and assert the JSON wire format. There are no per-handler unit tests — handler
  behavior is covered over HTTP.

## Test style

- Test methods use `// given` / `// when` / `// then` comment blocks and AssertJ.
- use `junit` to run tests, `assertj` for assertions, and `mockito` for mocks.
- AssertJ assertions carry a custom fail message stating what did not happen, e.g.
  `.withFailMessage(() -> "Client 'alice' was not removed from group 'producers'")`.
  Prefer the lazy `Supplier<String>` form so the message is only built on failure.
- `GatewayTestSupport` (kawa-integration-tests) is the shared lifecycle; subclasses override `authConfig()`/
  `rbacConfig()`/`initialTopics()`.

## Modules (dependency direction)

The build dependency direction is: `kawa-config` → `kawa-core` and
`kawa-protocol-kafka`; `kawa-virtual-topic` depends on core/config; `kawa-rbac` depends on
core/config/virtual-topic; `kawa-governance` depends on config; `kawa-http-admin` depends on
core/config/governance/virtual-topic; `kawa-server` assembles the runtime modules; and
`kawa-integration-tests` depends on `kawa-server`.

- `kawa-config`: `GatewayConfig`/`ResourceConfig`/`RbacConfig` + YAML
  `ConfigLoader` (plain Jackson; no custom `ResourceType` deserializer).
- `kawa-core`: `GatewayContext`, interceptors.
- `kawa-virtual-topic`: `VirtualTopicInterceptor` + per-API transforms; owns `VirtualTopicManager`
  (the virtual-to-physical mapping state).
- `kawa-protocol-kafka`: `KafkaApiRegistry` — the decoded API/version table; APIs not registered pass through ungated.
- `kawa-rbac`: `AuthorizationInterceptor` + per-API `AuthorizationCheck`s. One check per gate shape; RBAC is
  **unconditional** (no opt-out) and **default-deny**.
- `kawa-http-admin`: admin HTTP surface (e.g. `GET /topics`) exposing gateway state to a UI; reads
  `VirtualTopicManager`/`MetadataCache`, never talks to the broker.
- `kawa-server`: Netty server, broker clients, shading. Produces the runnable shaded jar
  (`mainClass=io.jonasg.kawa.server.GatewayLauncher`).
- `kawa-integration-tests`: real-client + raw-socket wire tests against a broker via
  `GatewayTestSupport`.

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