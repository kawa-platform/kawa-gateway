package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

import java.util.Map;

/// The `PUT /governance` request body. The rules and exemptions are unvalidated on purpose:
/// validating them here would throw from a Jackson-invoked constructor, and the parser wraps
/// that into an unreadable message carrying the internals it choked on. [GovernanceConfigMapper]
/// validates instead, so every rejection names the rule or exemption that caused it.
///
/// @param topicRules named rules, each a message plus a CEL expression
/// @param exemptions named exemptions, each a principal regex plus a topic pattern regex
@NullUnmarked
public record GovernanceConfigRequest(
        Map<String, GovernanceRuleRequest> topicRules,
        Map<String, GovernanceExemptionRequest> exemptions
) {

    public GovernanceConfigRequest {
        topicRules = topicRules == null ? Map.of() : topicRules;
        exemptions = exemptions == null ? Map.of() : exemptions;
    }
}
