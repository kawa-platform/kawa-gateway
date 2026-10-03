package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

import java.util.List;

/// A single governance rule in a request body, mirroring the shape of
/// [io.jonasg.kawa.config.GovernanceRuleConfig] with every value still a raw string.
/// Validated and converted by [GovernanceConfigMapper] rather than by this record.
///
/// A rule is one or more [subRules] combined with [match]. A body that sends only a top-level
/// [expression] (the shape before sub-rules) is still accepted: it becomes one check.
///
/// @param name         unique, human-readable name of the rule
/// @param errorMessage message shown when the rule rejects a resource
/// @param description  longer explanation of what the rule enforces
/// @param selector     which Kafka resources the rule applies to
/// @param match        `ALL` or `ANY`: how [subRules] combine
/// @param subRules     the checks and groups making up the rule
/// @param expression   legacy single expression; mutually exclusive with [subRules]
/// @param exemptions   named cases the rule does not apply to; `null` or absent means none
@NullUnmarked
public record GovernanceRuleRequest(
        String name,
        String errorMessage,
        String description,
        Selector selector,
        String match,
        List<SubRule> subRules,
        Expression expression,
        List<Exemption> exemptions
) {

    /// A sub-rule as sent: `kind` says which fields apply.
    ///
    /// @param kind         `check` or `group`
    /// @param name         unique among its siblings
    /// @param errorMessage optional; falls back to the nearest ancestor's message
    /// @param expression   a check's expression
    /// @param match        a group's `ALL` or `ANY`
    /// @param checks       a group's checks; each has `kind` `check`, never `group`
    @NullUnmarked
    public record SubRule(
            String kind,
            String name,
            String errorMessage,
            Expression expression,
            String match,
            List<SubRule> checks
    ) {
    }

    /// @param name        unique name of the exemption within its rule
    /// @param description why the exemption exists
    /// @param expression  expression that evaluates to `true` when the rule should be skipped
    @NullUnmarked
    public record Exemption(
            String name,
            String description,
            Expression expression
    ) {
    }

    /// @param resourceType Kafka resource type the rule targets: `TOPIC`, `GROUP` or `TRANSACTIONAL_ID`
    /// @param expression   narrows the selected resources; `null` selects them all
    /// @param scope        topics only: `BOTH` (default), `PHYSICAL` or `VIRTUAL`
    /// @param operations   topics only: `CREATE` and/or `ALTER`; absent means `[CREATE]`
    @NullUnmarked
    public record Selector(
            String resourceType,
            Expression expression,
            String scope,
            List<String> operations
    ) {
    }

    /// @param type  expression language, e.g. `CEL`
    /// @param value the expression source
    @NullUnmarked
    public record Expression(
            String type,
            String value
    ) {
    }
}
