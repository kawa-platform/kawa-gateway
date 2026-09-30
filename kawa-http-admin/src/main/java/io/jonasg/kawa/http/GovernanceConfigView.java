package io.jonasg.kawa.http;

import java.util.List;

/// The governance section in the `GET /governance/rules` response.
///
/// @param rules every governance rule, each with its own exemptions
public record GovernanceConfigView(
        List<GovernanceRuleConfigView> rules
) {
}
