package io.jonasg.kawa.http;

import java.util.List;

/// The governance section in the `GET /governance/rules` response.
///
/// @param rules      every governance rule
/// @param exemptions every exemption, each a principal regex plus a topic pattern regex
public record GovernanceConfigView(
        List<GovernanceRuleConfigView> rules,
        List<GovernanceExemptionConfigView> exemptions
) {
}
