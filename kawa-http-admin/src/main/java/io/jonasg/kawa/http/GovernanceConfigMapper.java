package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceVariableConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.GovernanceRequest;
import io.jonasg.kawa.governance.GovernanceTrace;
import org.apache.kafka.common.resource.ResourceType;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// Converts between the `/governance` transport types and the `kawa-config` governance records,
/// in both directions, and owns all input validation for governance requests: a rule request is
/// checked here (required fields, supported resource and expression types, CEL compilation) and
/// rejected with an [IllegalArgumentException] naming the rule and the field at fault, which the
/// handler turns into a `400`. The request records themselves stay unvalidated, because a check
/// thrown from a Jackson-invoked constructor surfaces as an unreadable parser message.
///
/// A stateless instance created once in [AdminHttpServer] and shared by the governance handlers.
@NullUnmarked
final class GovernanceConfigMapper {

    /// The governance section as the response body.
    GovernanceConfigView toGovernanceConfigView(GovernanceConfig config) {
        return new GovernanceConfigView(
                config.rules().values().stream()
                        .map(this::toGovernanceRuleConfigView)
                        .toList(),
                config.exemptions().values().stream()
                        .map(this::toGovernanceExemptionView)
                        .toList(),
                config.variables().values().stream()
                        .map(this::toGovernanceVariableConfigView)
                        .toList()
        );
    }

    /// A dry-run trace as the response body.
    GovernanceDryRunView toGovernanceDryRunView(GovernanceTrace trace) {
        return new GovernanceDryRunView(trace.allowed(), trace.exemptedBy(), trace.rules());
    }

    /// A dry-run body as the request governance evaluates. Only the fields the resource type
    /// binds are kept: unknown ones are dropped, and numbers and configs are normalised.
    ///
    /// @throws IllegalArgumentException if the resource type or the resource's name is missing
    GovernanceRequest toGovernanceRequest(@Nullable GovernanceDryRunRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("governance dry-run: body must not be empty");
        }
        var raw = req.resourceType();
        ResourceType resourceType = null;
        for (var candidate : List.of(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID)) {
            if (candidate.name().equalsIgnoreCase(raw == null ? "" : raw)) {
                resourceType = candidate;
            }
        }
        if (resourceType == null) {
            throw new IllegalArgumentException("governance dry-run: unsupported resourceType '" + raw + "'");
        }
        var given = req.resource() == null ? Map.<String, Object>of() : req.resource();
        boolean virtual = resourceType == ResourceType.TOPIC && Boolean.TRUE.equals(req.virtual());
        var operation = req.operation() == null
                ? GovernanceRuleConfig.Operation.CREATE
                : operation("governance dry-run: ", "operation", req.operation());
        var resource = new HashMap<String, Object>();
        if (resourceType == ResourceType.TOPIC) {
            resource.put("name", requireText(given, "name"));
            if (virtual) {
                resource.put("physicalTopic", given.get("physicalTopic") == null ? "" : given.get("physicalTopic").toString());
            } else {
                resource.put("partitions", intOr(given, "partitions"));
                resource.put("replicationFactor", intOr(given, "replicationFactor"));
                var configs = new HashMap<String, String>();
                if (given.get("configs") instanceof Map<?, ?> map) {
                    map.forEach((k, v) -> {
                        if (k != null && v != null) {
                            configs.put(k.toString(), v.toString());
                        }
                    });
                }
                resource.put("configs", configs);
            }
        } else {
            resource.put("id", requireText(given, "id"));
        }
        return new GovernanceRequest(resourceType, operation, virtual, resource, req.principal(), req.service());
    }

    private static String requireText(Map<String, Object> resource, String key) {
        var value = resource.get(key);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException("governance dry-run: resource." + key + " must not be blank");
        }
        return value.toString();
    }

    /// `-1` (the broker default) when absent, like a create request without the field.
    private static int intOr(Map<String, Object> resource, String key) {
        var value = resource.get(key);
        if (value == null) {
            return -1;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new IllegalArgumentException("governance dry-run: resource." + key + " must be a number");
    }

    /// A single governance variable as the response body.
    GovernanceVariableConfigView toGovernanceVariableConfigView(GovernanceVariableConfig config) {
        return new GovernanceVariableConfigView(
                config.name(),
                config.type().celName(),
                config.value(),
                config.note(),
                config.samples(),
                config.notes(),
                config.extraExamples());
    }

    /// A variable request as the config record. The name comes from the path; the type is
    /// matched strictly and the value must be a JSON literal of that type.
    ///
    /// @throws IllegalArgumentException on the first invalid field
    GovernanceVariableConfig toGovernanceVariableConfig(String name, @Nullable GovernanceVariableRequest req) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("governance variable: name must not be blank");
        }
        if (req == null) {
            throw new IllegalArgumentException("governance variable: body must not be empty");
        }
        var prefix = "governance variable '" + name + "': ";
        if (req.name() != null && !req.name().equals(name)) {
            throw new IllegalArgumentException(prefix + "body name '" + req.name() + "' does not match the path");
        }
        if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(prefix + "name must be a CEL identifier: letters, digits and _");
        }
        if (req.type() == null || req.type().isBlank()) {
            throw new IllegalArgumentException(prefix + "type must not be blank");
        }
        GovernanceVariableConfig.Type type = null;
        for (var candidate : GovernanceVariableConfig.Type.values()) {
            if (candidate.celName().equals(req.type().trim())) {
                type = candidate;
            }
        }
        if (type == null) {
            throw new IllegalArgumentException(prefix + "unsupported type '" + req.type() + "'");
        }
        if (req.value() == null || req.value().isBlank()) {
            throw new IllegalArgumentException(prefix + "value must not be blank");
        }
        var config = new GovernanceVariableConfig(
                name, type, req.value(), req.note(), req.samples(), req.notes(), req.extraExamples());
        GovernancePolicy.variableValue(config);
        return config;
    }

    /// A single governance rule as the response body.
    GovernanceRuleConfigView toGovernanceRuleConfigView(GovernanceRuleConfig config) {
        return new GovernanceRuleConfigView(
                config.name(),
                config.errorMessage(),
                config.description(),
                config.selector(),
                config.match(),
                config.subRules(),
                config.exemptions());
    }

    /// A single global exemption as the response body.
    GovernanceExemptionView toGovernanceExemptionView(GovernanceRuleConfig.Exemption config) {
        return new GovernanceExemptionView(config.name(), config.description(), config.expression());
    }

    /// A single rule request as a [GovernanceRuleConfig]. Checks run in a fixed order and fail on
    /// the first problem: the entry and its required fields, then the selector's resource type and
    /// scope, then the match and each sub-rule, then each expression's language and whether it
    /// compiles against the resource type's context.
    ///
    /// The rule's name always comes from the path. The body's `name` is optional; when present it
    /// must equal the path name, so a body can never rename or redirect the rule it is sent to.
    ///
    /// A body with a top-level `expression` and no `subRules` (the shape before sub-rules) becomes
    /// one check named after the rule.
    ///
    /// @throws IllegalArgumentException on the first invalid field, naming the rule and the field
    GovernanceRuleConfig toGovernanceRuleConfig(
            String name, @Nullable GovernanceRuleRequest req, Collection<GovernanceVariableConfig> variables) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("governance rule: name must not be blank");
        }
        if (req == null) {
            throw new IllegalArgumentException("governance rule: body must not be empty");
        }
        if (req.name() != null && !req.name().equals(name)) {
            throw new IllegalArgumentException(
                    "governance rule '" + name + "': body name '" + req.name() + "' does not match the path");
        }
        requireFields(name, req);
        var resourceType = resourceType(name, req.selector().resourceType());
        var scope = scope(name, resourceType, req.selector().scope());
        var selectorExpression = req.selector().expression() == null
                ? null
                : expression(name, resourceType, variables, "selector.expression", req.selector().expression());
        var selector = new GovernanceRuleConfig.Selector(
                resourceType, selectorExpression, scope, operations(name, resourceType, req.selector().operations()));
        var exemptions = exemptions(name, resourceType, variables, req.exemptions());
        if (req.subRules() == null || req.subRules().isEmpty()) {
            return new GovernanceRuleConfig(
                    name,
                    req.errorMessage(),
                    req.description(),
                    selector,
                    expression(name, resourceType, variables, "expression", req.expression()),
                    exemptions);
        }
        if (req.expression() != null) {
            throw new IllegalArgumentException(
                    "governance rule '" + name + "': set either expression or subRules, not both");
        }
        return new GovernanceRuleConfig(
                name,
                req.errorMessage(),
                req.description(),
                selector,
                match(name, "match", req.match()),
                subRules(name, resourceType, variables, req.subRules()),
                exemptions);
    }

    /// A global exemption request as the config record. The name comes from the path; the
    /// expression may read any resource variable.
    ///
    /// @throws IllegalArgumentException on the first invalid field
    GovernanceRuleConfig.Exemption toGovernanceExemption(
            String name, @Nullable GovernanceExemptionRequest req, Collection<GovernanceVariableConfig> variables) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("governance exemption: name must not be blank");
        }
        if (req == null) {
            throw new IllegalArgumentException("governance exemption: body must not be empty");
        }
        if (req.name() != null && !req.name().equals(name)) {
            throw new IllegalArgumentException(
                    "governance exemption '" + name + "': body name '" + req.name() + "' does not match the path");
        }
        if (req.expression() == null) {
            throw new IllegalArgumentException("governance exemption '" + name + "': expression must not be null");
        }
        var raw = req.expression();
        var prefix = "governance exemption '" + name + "': ";
        if (raw.type() == null || !raw.type().equalsIgnoreCase(GovernanceRuleConfig.Expression.Type.CEL.name())) {
            throw new IllegalArgumentException(prefix + "unsupported expression.type '" + raw.type() + "'");
        }
        if (raw.value() == null || raw.value().isBlank()) {
            throw new IllegalArgumentException(prefix + "expression.value must not be blank");
        }
        var error = GovernancePolicy.globalExemptionValidationError(raw.value(), variables);
        if (error.isPresent()) {
            throw new IllegalArgumentException(prefix + "expression: " + error.get());
        }
        return new GovernanceRuleConfig.Exemption(name, req.description(), GovernanceRuleConfig.Expression.cel(raw.value()));
    }

    /// Converts the rule's sub-rules, which may mix checks and groups. `path` prefixes field
    /// names in error messages, e.g. `subRules[app].checks[standard]`.
    private static List<GovernanceRuleConfig.SubRule> subRules(
            String rule, ResourceType resourceType, Collection<GovernanceVariableConfig> variables,
            List<GovernanceRuleRequest.SubRule> raw) {
        var names = new HashSet<String>();
        var subRules = new ArrayList<GovernanceRuleConfig.SubRule>();
        for (var subRule : raw) {
            var field = "subRules[" + subRuleName(rule, "subRules", subRule, names) + "]";
            var kind = subRule.kind();
            if ("group".equalsIgnoreCase(kind)) {
                subRules.add(group(rule, resourceType, variables, field, subRule));
            } else if ("check".equalsIgnoreCase(kind)) {
                subRules.add(check(rule, resourceType, variables, field, subRule));
            } else {
                throw new IllegalArgumentException("governance rule '" + rule + "': " + field
                        + ": kind must be 'check' or 'group', not '" + kind + "'");
            }
        }
        return List.copyOf(subRules);
    }

    private static GovernanceRuleConfig.SubRule.Group group(
            String rule, ResourceType resourceType, Collection<GovernanceVariableConfig> variables,
            String field, GovernanceRuleRequest.SubRule raw) {
        if (raw.expression() != null) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + ": a group has no expression");
        }
        if (raw.checks() == null || raw.checks().isEmpty()) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + ".checks must not be empty");
        }
        var names = new HashSet<String>();
        var checks = new ArrayList<GovernanceRuleConfig.SubRule.Check>();
        for (var check : raw.checks()) {
            var checkField = field + ".checks[" + subRuleName(rule, field + ".checks", check, names) + "]";
            if (!"check".equalsIgnoreCase(check.kind())) {
                throw new IllegalArgumentException("governance rule '" + rule + "': " + checkField
                        + ": a group holds checks only, not '" + check.kind() + "'");
            }
            checks.add(check(rule, resourceType, variables, checkField, check));
        }
        return new GovernanceRuleConfig.SubRule.Group(
                raw.name(), raw.errorMessage(), match(rule, field + ".match", raw.match()), checks);
    }

    private static GovernanceRuleConfig.SubRule.Check check(
            String rule, ResourceType resourceType, Collection<GovernanceVariableConfig> variables,
            String field, GovernanceRuleRequest.SubRule raw) {
        if (raw.match() != null || (raw.checks() != null && !raw.checks().isEmpty())) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + ": a check has no match or checks");
        }
        return new GovernanceRuleConfig.SubRule.Check(
                raw.name(), raw.errorMessage(), expression(rule, resourceType, variables, field + ".expression", raw.expression()));
    }

    /// The sub-rule's name, after checking it is present, non-blank and unique among `seen`.
    private static String subRuleName(
            String rule, String list, GovernanceRuleRequest.SubRule raw, Set<String> seen) {
        if (raw == null) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + list + " must not contain null");
        }
        var name = raw.name();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + list + ": sub-rule name must not be blank");
        }
        if (!seen.add(name)) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': " + list + ": duplicate sub-rule name '" + name + "'");
        }
        return name;
    }

    private static GovernanceRuleConfig.Match match(String rule, String field, String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + " must not be blank");
        }
        for (var match : GovernanceRuleConfig.Match.values()) {
            if (match.name().equalsIgnoreCase(raw)) {
                return match;
            }
        }
        throw new IllegalArgumentException("governance rule '" + rule + "': unsupported " + field + " '" + raw + "'");
    }

    /// Absent or empty means `[CREATE]`. Only topic rules may run on `ALTER` or `DELETE`.
    private static Set<GovernanceRuleConfig.Operation> operations(
            String rule, ResourceType resourceType, @Nullable List<String> raw) {
        var operations = new HashSet<GovernanceRuleConfig.Operation>();
        for (var value : raw == null ? List.<String>of() : raw) {
            operations.add(operation("governance rule '" + rule + "': ", "selector.operations", value));
        }
        if (resourceType != ResourceType.TOPIC) {
            for (var operation : operations) {
                if (operation != GovernanceRuleConfig.Operation.CREATE) {
                    throw new IllegalArgumentException(
                            "governance rule '" + rule + "': selector.operations '" + operation + "' only applies to TOPIC rules");
                }
            }
        }
        return operations;
    }

    private static GovernanceRuleConfig.Operation operation(String prefix, String field, @Nullable String raw) {
        for (var candidate : GovernanceRuleConfig.Operation.values()) {
            if (candidate.name().equalsIgnoreCase(raw == null ? "" : raw.trim())) {
                return candidate;
            }
        }
        throw new IllegalArgumentException(prefix + "unsupported " + field + " '" + raw + "'");
    }

    /// `null` means [GovernanceRuleConfig.TopicScope#BOTH]. Only topic rules may narrow it.
    private static GovernanceRuleConfig.TopicScope scope(String rule, ResourceType resourceType, String raw) {
        if (raw == null) {
            return GovernanceRuleConfig.TopicScope.BOTH;
        }
        GovernanceRuleConfig.TopicScope scope = null;
        for (var candidate : GovernanceRuleConfig.TopicScope.values()) {
            if (candidate.name().equalsIgnoreCase(raw)) {
                scope = candidate;
            }
        }
        if (scope == null) {
            throw new IllegalArgumentException("governance rule '" + rule + "': unsupported selector.scope '" + raw + "'");
        }
        if (resourceType != ResourceType.TOPIC && scope != GovernanceRuleConfig.TopicScope.BOTH) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': selector.scope '" + raw + "' only applies to TOPIC rules");
        }
        return scope;
    }

    /// Checks each exemption in order: not null, a non-blank name unique within the rule, and a
    /// supported, compiling expression. A `null` list means the rule has no exemptions.
    private static List<GovernanceRuleConfig.Exemption> exemptions(
            String rule, ResourceType resourceType, Collection<GovernanceVariableConfig> variables,
            @Nullable List<GovernanceRuleRequest.Exemption> raw) {
        if (raw == null) {
            return List.of();
        }
        var names = new HashSet<String>();
        var exemptions = new ArrayList<GovernanceRuleConfig.Exemption>();
        for (var exemption : raw) {
            if (exemption == null) {
                throw new IllegalArgumentException("governance rule '" + rule + "': exemption must not be null");
            }
            var name = exemption.name();
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("governance rule '" + rule + "': exemption name must not be blank");
            }
            if (!names.add(name)) {
                throw new IllegalArgumentException(
                        "governance rule '" + rule + "': duplicate exemption name '" + name + "'");
            }
            if (exemption.expression() == null) {
                throw new IllegalArgumentException(
                        "governance rule '" + rule + "': exemption '" + name + "': expression must not be null");
            }
            exemptions.add(new GovernanceRuleConfig.Exemption(
                    name,
                    exemption.description(),
                    expression(rule, resourceType, variables, "exemption '" + name + "' expression", exemption.expression())));
        }
        return List.copyOf(exemptions);
    }

    private static void requireFields(String name, GovernanceRuleRequest req) {
        if (req.errorMessage() == null || req.errorMessage().isBlank()) {
            throw new IllegalArgumentException("governance rule '" + name + "': errorMessage must not be blank");
        }
        if (req.selector() == null) {
            throw new IllegalArgumentException("governance rule '" + name + "': selector must not be null");
        }
        if (req.expression() == null && (req.subRules() == null || req.subRules().isEmpty())) {
            throw new IllegalArgumentException("governance rule '" + name + "': subRules must not be empty");
        }
    }

    /// `TOPIC`, `GROUP` or `TRANSACTIONAL_ID`, matched explicitly: Kafka's `ResourceType.fromString`
    /// maps anything it does not recognise to `UNKNOWN` instead of failing.
    private static ResourceType resourceType(String rule, String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': selector.resourceType must not be blank");
        }
        for (var candidate : List.of(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID)) {
            if (candidate.name().equalsIgnoreCase(raw) && GovernancePolicy.governs(candidate)) {
                return candidate;
            }
        }
        throw new IllegalArgumentException(
                "governance rule '" + rule + "': unsupported selector.resourceType '" + raw + "'");
    }

    private static GovernanceRuleConfig.Expression expression(
            String rule, ResourceType resourceType, Collection<GovernanceVariableConfig> variables,
            String field, GovernanceRuleRequest.Expression raw) {
        if (raw == null) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + " must not be null");
        }
        if (raw.type() == null || !raw.type().equalsIgnoreCase(GovernanceRuleConfig.Expression.Type.CEL.name())) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': unsupported " + field + ".type '" + raw.type() + "'");
        }
        if (raw.value() == null || raw.value().isBlank()) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': " + field + ".value must not be blank");
        }
        var error = GovernancePolicy.validationError(resourceType, raw.value(), variables);
        if (error.isPresent()) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + ": " + error.get());
        }
        return GovernanceRuleConfig.Expression.cel(raw.value());
    }
}
