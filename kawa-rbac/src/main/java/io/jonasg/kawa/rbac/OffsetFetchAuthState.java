package io.jonasg.kawa.rbac;

import java.util.LinkedHashSet;
import java.util.Set;

/// Per-request state for OffsetFetch authorization: remembers which topics were denied on the
/// per-topic TOPIC DESCRIBE gate, so the response can be reconstructed with a
/// TOPIC_AUTHORIZATION_FAILED entry per denied topic once the broker replies for the authorized
/// ones, or that the request named no topics (fetch-all), so the response must be filtered. The
/// GROUP DESCRIBE gate never needs state - when it fails the whole request is short-circuited
/// immediately.
final class OffsetFetchAuthState {

    private final Set<String> deniedTopics = new LinkedHashSet<>();
    private boolean allTopics;

    void recordAllTopics() {
        allTopics = true;
    }

    boolean allTopics() {
        return allTopics;
    }

    void recordDenied(String topic) {
        deniedTopics.add(topic);
    }

    Set<String> deniedTopics() {
        return Set.copyOf(deniedTopics);
    }
}
