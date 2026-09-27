package io.jonasg.kawa.http;

/// A single governance exemption in a `PUT /governance` body, validated by
/// [GovernanceConfigMapper] rather than by this record.
///
/// @param principal    regex matched against the requesting principal
/// @param topicPattern regex matched against the topic name
public record GovernanceExemptionRequest(String principal, String topicPattern) {
}
