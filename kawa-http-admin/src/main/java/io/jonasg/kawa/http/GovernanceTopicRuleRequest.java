package io.jonasg.kawa.http;

import org.jspecify.annotations.Nullable;

/// The `PUT /governance/rules/topics/{name}` request body.
public record GovernanceTopicRuleRequest(
        String name,
        String errorMessage,
        String description,
        Selector selector,
        Expression expression
) {

    record Selector(
            String resourceType,
            @Nullable Expression value
    ) { }

    record Expression(
            String type,
            String value
    ) { }
}
