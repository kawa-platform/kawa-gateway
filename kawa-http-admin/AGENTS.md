# kawa-http-admin agent instructions

## Scope

These instructions apply to the `kawa-http-admin` module only.

## Handler naming

Name HTTP handlers after the HTTP method they own: `Get`, `Post`, `Put`, `Patch`, or `Delete`,
followed by the resource name. Use singular or plural resource names according to the route's
resource shape. Keep handlers method-specific rather than combining multiple HTTP methods in one
class.

## Request flow

Every endpoint that writes config follows the same path:

```text
handler:  parse body -> mapper.toXConfig(pathName, request)  // all input checks; throws -> 400
          -> service.upsertX(config, consistency)
          -> mapper.toXView(config)
service:  updater.update(consistency, cfg -> cfg.upsertX(config))
```

- **Handlers** own the HTTP concerns: parsing, the path name, status codes, and calling the mapper on both sides.
- **Mappers** convert between transport types and `kawa-config` records, one mapper per section (e.g.
  `GovernanceConfigMapper`). A mapper is a stateless instance created once in `AdminHttpServer` and injected into
  the handlers that need it — never static, never `new`-ed inside a service — so it can take collaborators later
  without rewiring callers.
- **Mapper methods are named after the full type they return**: `to` + the simple type name, so a method producing
  `FooView` is `toFooView`, never a bare `toView` or `toConfig`. Hence `toGovernanceRuleConfig(...)` for the rule
  config, `toGovernanceRuleConfigView(GovernanceRuleConfig)` for its view and
  `toGovernanceConfigView(GovernanceConfig)` for the whole section. The name then says what comes back without
  reading the signature, and a mapper can convert to several types without relying on overloads.
- **Services** accept and return `kawa-config` records only, never transport types. They own the read-modify-write
  against the config repository and the requested `Consistency`, nothing else.
- The name of a `PUT /{name}` entry comes from the path and is passed to the mapper; a body never decides it.
- `BaseCRUDHandler` sections (roles, groups, clients) convert at its `toConfig` and `listView`/`putView` hooks; those
  hooks delegate to the section's mapper.

### Validation

- The mapper performs all input validation and throws `IllegalArgumentException`, which the handler turns into a
  `400`. Fail on the first problem, checking missing/blank fields first, then enum values, then deeper checks such as
  compiling a CEL expression (`GovernancePolicy.validationError`).
- Convert strictly. Do not use lenient helpers that swallow bad input — e.g. Kafka's `ResourceType.fromString`
  returns `UNKNOWN` — nor raw `Enum.valueOf`, which is case-sensitive and leaks an unhelpful message. Match the
  allowed values explicitly.
- Error messages name the entry and the field at fault (`governance rule 'x': unsupported resourceType 'FOO'`) and
  never the parser's internals.
- Request records stay unvalidated on purpose: a check thrown from a Jackson-invoked constructor surfaces as an
  unreadable parser message. Validation belongs in the mapper, after parsing.
- The `kawa-config` record constructors keep their own invariants as a backstop; a user should never see their
  messages because the mapper rejects first.
- Test validation through the slice tests: each rejection case is a request with a bad body asserting the `400` and
  its message (see [HTTP tests](#http-tests)).

## Transport type naming

Every admin HTTP request and response body is a transport type declared in `io.jonasg.kawa.http`, never a
`kawa-config` record. The config records stay the domain: the config repository stores them, and mappers convert
between the two. For `BaseCRUDHandler<C, R, G, P>`, `C` is the config record a section stores, `R` the body the
endpoint accepts, `G` the `GET` list element and `P` the `PUT` response body — the last two named after the method
that produces them, so neither hook returns an untyped value. Because a `PUT` response is the same view as a list
element (see below), `G` and `P` are the same type.

The suffix says which side of the boundary a type sits on, and the set is stable: `*Request` and `*Patch` are bodies
a client sends, and `*View` a response body mirroring the config record it is named after.

Every view of a named entry includes its `name`, including the one a `PUT /{name}` endpoint writes back: a response
must be self-describing without the request path next to it. A `PUT /{name}` therefore returns the same `*View` as
that entry's element in the `GET` list — `ClientConfigView` for both `GET /auth/clients` and
`PUT /auth/clients/{name}`, `GovernanceRuleConfigView` for both `GET /governance/rules` and
`PUT /governance/rules/{name}`. There is no separate `*PutView`; `RolePutView` and `GroupPutView` predate this rule
and are to be replaced by `RoleConfigView` and `GroupConfigView`.

The name in a response always comes from the stored entry; on the request side it comes from the path (see
[Request flow](#request-flow)).

The rule covers the **top-level** body only. Records nested inside a body are left as `kawa-config` types, because
they are an interior detail of the payload and copying them would duplicate the whole `kawa-config` graph under
new names. That is why `VirtualTopicConfigView` still exposes `VirtualTopicFilterConfig` and `PayloadFormatConfig`.

- A `*View` must never carry a secret. `ClientConfigView` is `(username, mechanism)`; a password belongs only to
  `ClientConfigRequest` and `ClientConfigPatch`.
- `TopicRequest` is named for its `type` discriminator, which names the route resource (`physical` or `virtual`)
  rather than a config record. Hence `TopicRequest`, not `TopicConfigRequest`.
- Null-defaulting in a mapper's `to<Config>` method must replicate the config record it replaces, exactly. A body the config
  record accepted as empty must still be accepted, or the accept/reject boundary moves and a client that used to
  get `200` gets `400`. This is the easiest part of a transport type to get wrong.

## HTTP tests

- Slice tests are the default way to test this module. Handlers, mappers and services are covered through them:
  drive the real endpoint and assert the status, the JSON body and the persisted snapshot. Do not write separate
  unit tests for individual mappers or services in most cases — they would repeat what the slice test already
  proves and pin the internal split between handler, mapper and service.
- Every handler gets its own slice test, named after the handler with `Handler` replaced by `SliceTest`:
  `PutGovernanceRuleHandler` → `PutGovernanceRuleSliceTest`,
  `GetGovernanceRulesHandler` → `GetGovernanceRulesSliceTest`. A slice test covers exactly one HTTP method on one
  route; it may call other endpoints only to set up state or to read back the result.
- Add a mapper or service unit test only when a case cannot reasonably be reached or set up over HTTP, and say why
  in the test.
- HTTP slice tests must exercise the real `AdminHttpServer` through an ephemeral port, following
  `AdminHttpSliceTestBase`.
- Assert the JSON wire format and HTTP status codes consumed by the admin UI.
- Use `json-unit`'s AssertJ integration (`assertThatJson`) for structured JSON response assertions instead of asserting
  individual JSON fragments with
  `contains`.
- Use `IGNORING_ARRAY_ORDER` when collection ordering is not part of the behavior under test; do not ignore extra fields
  unless the test explicitly permits them.
- Use `// given`, `// when`, and `// then` sections with AssertJ assertions.
- Write multiline JSON request bodies as Java text blocks (`"""..."""`), not concatenated string literals.
- Keep handler-only tests for pure handler behavior; do not replace slice coverage when testing routing, serialization,
  server bootstrap, or CORS.
- Use `Req` and `Resp` as suffixes for paired request/response variables and fixtures and use java `var`:

  ```java
  var createTopicReq = request("POST", "/topics", body);
  var createTopicResp = send(createTopicReq);
  ```

- Use operation-specific names such as `createTopicReq`, `createTopicResp`,
  `deleteClientReq`, and `deleteClientResp`.
- For a simple one-shot request where constructing a request object adds no value, a descriptive response variable such
  as `topicsResp` is sufficient.
- Prefer `request` and `response` in production APIs and framework types;
  `Req`/`Resp` is a test naming convention, not an API naming convention.

## Test method naming

Use descriptive, behavior-focused method names. For the standard CRUD lifecycle in slice tests,
follow the existing patterns:

- First create/upsert via `PUT /{name}`: `adds<Resource>AndPersistsSnapshot()`
  - e.g. `addsRoleAndPersistsSnapshot()`, `addsGroupAndPersistsSnapshot()`,
    `addsClientAndPersistsSnapshot()`.
- Listing an empty or populated section: `listsConfigured<Resource>s()`,
  `lists<Resource>sEmptyWhenNoSnapshotApplied()`.
- Deleting an existing entry: `removes<Resource>AndPersistsSnapshot()`.
- Deleting a missing entry: `missing<Resource>ReturnsNotFound()`.

## Verification

- Unit and HTTP slice tests use Surefire and can be run with:

  ```text
  ./mvnw -pl kawa-http-admin -am test
  ```

- Keep HTTP slice tests separate from the pure plumbing tests such as
  `RouterTest` and `KafkaTopicAdminTest`.

## Style

- Match the surrounding file's indentation and formatting.
- Prefer small, behavior-focused tests with descriptive test method names.
- Do not introduce a new abstraction when an existing helper or base class already covers the use case.
