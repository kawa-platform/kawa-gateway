# kawa-http-admin agent instructions

## Scope

These instructions apply to the `kawa-http-admin` module only.

## Handler naming

Name HTTP handlers after the HTTP method they own: `Get`, `Post`, `Put`, `Patch`, or `Delete`,
followed by the resource name. Use singular or plural resource names according to the route's
resource shape. Keep handlers method-specific rather than combining multiple HTTP methods in one
class.

## Transport type naming

Every admin HTTP request and response body is a transport type declared in `io.jonasg.kawa.http`, never a
`kawa-config` record. The config records stay the domain: the config repository stores them, and handlers convert
between the two at the `toConfig` and `listView`/`putView` hooks of `BaseCRUDHandler<T, R>`, where `T` is the config
record a section stores and `R` is the body the endpoint accepts.

The rule covers the **top-level** body only. Records nested inside a body are left as `kawa-config` types, because
they are an interior detail of the payload and copying them would duplicate the whole `kawa-config` graph under
new names. That is why governance has a `GovernanceConfigView` but no rule or exemption view type of its own, and
why `VirtualTopicConfigView` still exposes `VirtualTopicFilterConfig` and `PayloadFormatConfig`.

Governance is the deliberate end of that carve-out. Its rules and exemptions round-trip through the admin UI
unchanged, so their wire shape is frozen and a view type could only be a copy. The nested governance schemas in
`openapi.yaml` are consequently still named after the `kawa-config` records on both sides of the boundary, even
though the `PUT` request body carries the unvalidating `GovernanceRuleRequest` and `GovernanceExemptionRequest` in
place of the config records.

- A `*View` must never carry a secret. `ClientConfigView` is `(username, mechanism)`; a password belongs only to
  `ClientConfigRequest` and `ClientConfigPatch`.
- `TopicRequest` is named for its `type` discriminator, which names the route resource (`physical` or `virtual`)
  rather than a config record. Hence `TopicRequest`, not `TopicConfigRequest`.
- Null-defaulting on a request record must replicate the config record it replaces, exactly. A body the config
  record accepted as empty must still deserialize, or the accept/reject boundary moves and a client that used to
  get `200` gets `400`. This is the `BaseCRUDHandler.toConfig` contract, and it is the easiest part of a transport
  type to get wrong.

## HTTP tests

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
