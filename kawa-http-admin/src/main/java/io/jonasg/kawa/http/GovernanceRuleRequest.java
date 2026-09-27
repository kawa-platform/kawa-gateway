package io.jonasg.kawa.http;

/// A single topic governance rule in a `PUT /governance` body, validated by
/// [GovernanceConfigMapper] rather than by this record.
///
/// @param message    description shown to the user when this rule rejects a topic
/// @param expression CEL expression evaluated against the topic bindings — must return boolean
public record GovernanceRuleRequest(String message, String expression) {
}
