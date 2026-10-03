package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

/// A global governance exemption in a `PUT /governance/exemptions/{name}` body. Validated and
/// converted by [GovernanceConfigMapper].
///
/// @param name        optional; must equal the path name when present
/// @param description why the exemption exists
/// @param expression  expression that evaluates to `true` when every rule should be skipped
@NullUnmarked
public record GovernanceExemptionRequest(
        String name,
        String description,
        GovernanceRuleRequest.Expression expression
) {
}
