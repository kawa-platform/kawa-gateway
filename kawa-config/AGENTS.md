# kawa-config agent instructions

## Scope

These instructions apply to the `kawa-config` module only.

## Config mutation naming

Config records are immutable; mutation methods return a new instance. Name them by the
operation they perform:

- `upsertX(name, value)` — add-or-replace one entry in a map-backed record
  (`RbacConfig.upsertRole`, `AuthConfig.upsertClient`, `GovernanceConfig.upsertRule`,
  `DynamicConfig.upsertVirtualTopic`).
- `removeX(name)` — remove one entry by key (`RbacConfig.removeRole`,
  `DynamicConfig.removeVirtualTopic`).
- `renameX(from, to)` — re-key an entry (`RbacConfig.renameGroup`).
- `updateX(section)` — replace a whole section (`DynamicConfig.updateAuth`,
  `DynamicConfig.updateRbac`).