package io.jonasg.kawa.http;

/// A single governance exemption in an admin `/governance` response. Evaluation is skipped when
/// both regexes match: the requesting principal and the topic name.
///
/// @param principal    regex matched against the requesting principal
/// @param topicPattern regex matched against the topic name
public record GovernanceExemptionConfigView(String principal, String topicPattern) {
}
