package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceRuleConfig;

/// A global governance exemption in an admin `/governance/exemptions` response.
///
/// @param name        unique name among global exemptions
/// @param description why the exemption exists
/// @param expression  expression that evaluates to `true` when every rule should be skipped
public record GovernanceExemptionView(
        String name,
        String description,
        GovernanceRuleConfig.Expression expression
) {
}
