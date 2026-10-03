package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/// A single governance rule: a human-readable unique [name], an [errorMessage] shown when the
/// rule is violated, a [description] explaining the rule in more detail, a [selector] that
/// determines the Kafka resource(s) the rule applies to, and one or more [subRules] combined
/// with [match]. The rule holds when the combination evaluates to `true`.
///
/// A sub-rule is a standalone [SubRule.Check] or a [SubRule.Group] of checks with its own
/// [Match]; groups hold checks only, so they nest one level. The rule itself is the top-level
/// group: its [match] and [subRules] may mix checks and groups. [#expression()] is the single
/// CEL expression the tree is equivalent to.
///
/// @param name         human-readable unique name of the rule
/// @param errorMessage human-readable message shown when the rule is violated
/// @param description  human-readable description of the rule
/// @param selector     determines the Kafka resource(s) the rule applies to
/// @param match        how [subRules] combine: all must hold, or any one; never `null`
/// @param subRules     the checks and groups making up the rule; never empty
/// @param exemptions   named cases this rule does not apply to; never `null`, empty when there are none
public record GovernanceRuleConfig(
        String name,
        String errorMessage,
        String description,
        Selector selector,
        Match match,
        List<SubRule> subRules,
        List<Exemption> exemptions
) {

    /// A rule that is one check, without exemptions.
    public GovernanceRuleConfig(
            String name,
            String errorMessage,
            String description,
            Selector selector,
            Expression expression
    ) {
        this(name, errorMessage, description, selector, expression, List.of());
    }

    /// A rule that is one check, named after the rule.
    public GovernanceRuleConfig(
            String name,
            String errorMessage,
            String description,
            Selector selector,
            Expression expression,
            List<Exemption> exemptions
    ) {
        this(name, errorMessage, description, selector, Match.ALL, single(name, expression), exemptions);
    }

    public GovernanceRuleConfig {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        if (subRules == null || subRules.isEmpty()) {
            throw new IllegalArgumentException("governance rule '" + name + "': subRules must not be empty");
        }
        match = match == null ? Match.ALL : match;
        subRules = List.copyOf(subRules);
        requireUniqueNames(name, subRules);
        exemptions = exemptions == null ? List.of() : List.copyOf(exemptions);
    }

    /// Reads a stored rule. Snapshots written before sub-rules existed carry a single top-level
    /// `expression`; it becomes one check named after the rule.
    @JsonCreator
    static GovernanceRuleConfig fromJson(
            @JsonProperty("name") String name,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("description") String description,
            @JsonProperty("selector") Selector selector,
            @JsonProperty("match") Match match,
            @JsonProperty("subRules") List<SubRule> subRules,
            @JsonProperty("expression") Expression legacyExpression,
            @JsonProperty("exemptions") List<Exemption> exemptions
    ) {
        if ((subRules == null || subRules.isEmpty()) && legacyExpression != null) {
            return new GovernanceRuleConfig(name, errorMessage, description, selector, legacyExpression, exemptions);
        }
        return new GovernanceRuleConfig(name, errorMessage, description, selector, match, subRules, exemptions);
    }

    /// The single CEL expression this rule's sub-rules are equivalent to: checks joined with
    /// `&&` or `||` per [Match], groups parenthesised. A rule that is one check returns that
    /// check's expression unchanged. Not a record component, so it is never serialized; the
    /// legacy `expression` property is only read, see [#fromJson].
    public Expression expression() {
        return Expression.cel(SubRule.cel(match, subRules));
    }

    private static List<SubRule> single(String name, Expression expression) {
        if (expression == null) {
            throw new IllegalArgumentException("expression must not be null");
        }
        return List.of(SubRule.check(name, expression));
    }

    private static void requireUniqueNames(String parent, List<? extends SubRule> subRules) {
        var names = new HashSet<String>();
        for (var subRule : subRules) {
            if (!names.add(subRule.name())) {
                throw new IllegalArgumentException("'" + parent + "': duplicate sub-rule name '" + subRule.name() + "'");
            }
        }
    }

    /// How a rule or group combines its sub-rules.
    public enum Match {
        /// Every sub-rule must hold (`&&`).
        ALL,
        /// At least one sub-rule must hold (`||`).
        ANY
    }

    /// The topic requests a rule runs on: [#CREATE] for CreateTopics, [#ALTER] for config
    /// changes and CreatePartitions, judged against the topic as it will be after the change,
    /// and [#DELETE] for deleting it, judged against the topic as it is.
    public enum Operation {
        CREATE,
        ALTER,
        DELETE
    }

    /// Which topics a topic rule covers. Rules for other resource types are always [#BOTH].
    public enum TopicScope {
        BOTH,
        PHYSICAL,
        VIRTUAL
    }

    /// One part of a rule: a standalone [Check], or a [Group] of checks. A group holds checks
    /// only, so groups nest one level by construction. The `kind` property carries the type on
    /// the wire: `check` or `group`.
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SubRule.Check.class, name = "check"),
            @JsonSubTypes.Type(value = SubRule.Group.class, name = "group")
    })
    public sealed interface SubRule permits SubRule.Check, SubRule.Group {

        /// Unique among its siblings.
        String name();

        /// Optional; when `null` the nearest ancestor's message applies.
        String errorMessage();

        static Check check(String name, Expression expression) {
            return new Check(name, null, expression);
        }

        static Group group(String name, Match match, List<Check> checks) {
            return new Group(name, null, match, checks);
        }

        /// A standalone CEL check.
        ///
        /// @param name         unique among its siblings
        /// @param errorMessage optional; when `null` the nearest ancestor's message applies
        /// @param expression   the check; never `null`
        record Check(String name, String errorMessage, Expression expression) implements SubRule {

            public Check {
                requireName(name);
                if (expression == null) {
                    throw new IllegalArgumentException("sub-rule '" + name + "': expression must not be null");
                }
            }
        }

        /// Checks combined with [match].
        ///
        /// @param name         unique among its siblings
        /// @param errorMessage optional; when `null` the nearest ancestor's message applies
        /// @param match        how the checks combine; never `null`
        /// @param checks       one or more checks; a group never contains another group
        record Group(String name, String errorMessage, Match match, List<Check> checks) implements SubRule {

            public Group {
                requireName(name);
                if (match == null) {
                    throw new IllegalArgumentException("sub-rule '" + name + "': match must not be null");
                }
                if (checks == null || checks.isEmpty()) {
                    throw new IllegalArgumentException("sub-rule '" + name + "': a group needs at least one check");
                }
                checks = List.copyOf(checks);
                requireUniqueNames(name, checks);
            }
        }

        private static void requireName(String name) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("sub-rule name must not be null or blank");
            }
        }

        /// The CEL expression `subRules` combined with `match` are equivalent to.
        static String cel(Match match, List<? extends SubRule> subRules) {
            if (subRules.isEmpty()) {
                return match == Match.ALL ? "true" : "false";
            }
            var parts = subRules.stream()
                    .map(subRule -> switch (subRule) {
                        case Check check -> check.expression().value();
                        case Group group -> cel(group.match(), group.checks());
                    })
                    .toList();
            if (parts.size() == 1) {
                return parts.getFirst();
            }
            var operator = match == Match.ALL ? " && " : " || ";
            return String.join(operator, parts.stream().map(p -> "(" + p + ")").toList());
        }
    }

    /// A named case the rule does not apply to: when [#expression] evaluates to `true` for a
    /// request, the rule is skipped for that request. The same shape serves global exemptions
    /// ([GovernanceConfig#exemptions]), which skip every rule.
    ///
    /// @param name        unique name of the exemption within its rule (or among global exemptions)
    /// @param description why the exemption exists
    /// @param expression  expression that evaluates to `true` when the rule should be skipped
    public record Exemption(
            String name,
            String description,
            Expression expression
    ) {
    }

    /// Which resources a rule applies to: a resource type, for topics a [TopicScope] and the
    /// [Operation]s it runs on, and an optional narrowing expression (`null` selects every
    /// resource of that type).
    ///
    /// @param resourceType `TOPIC`, `GROUP` or `TRANSACTIONAL_ID`
    /// @param expression   narrows the selected resources; `null` selects them all
    /// @param scope        physical topics, virtual topics or both; never `null`, [TopicScope#BOTH] by default
    /// @param operations   create, alter or both; never empty, `[CREATE]` by default
    public record Selector(
            ResourceType resourceType,
            Expression expression,
            TopicScope scope,
            Set<Operation> operations
    ) {

        public Selector(ResourceType resourceType, Expression expression) {
            this(resourceType, expression, TopicScope.BOTH, null);
        }

        public Selector(ResourceType resourceType, Expression expression, TopicScope scope) {
            this(resourceType, expression, scope, null);
        }

        public Selector {
            scope = scope == null ? TopicScope.BOTH : scope;
            operations = operations == null || operations.isEmpty()
                    ? Set.of(Operation.CREATE)
                    : Collections.unmodifiableSet(EnumSet.copyOf(operations));
        }

        public static Selector topic(Expression exp) {
            return new Selector(ResourceType.TOPIC, exp);
        }

        public static Selector topic() {
            return new Selector(ResourceType.TOPIC, null);
        }
    }

    public record Expression(
            Type type,
            String value
    ) {
        public enum Type {
            CEL
        }

        public static Expression cel(String value) {
            return new Expression(Type.CEL, value);
        }
    }
}
