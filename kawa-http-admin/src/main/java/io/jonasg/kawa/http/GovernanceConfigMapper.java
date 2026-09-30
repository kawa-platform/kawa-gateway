package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import org.apache.kafka.common.resource.ResourceType;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
    GovernanceConfigView toGovernanceRuleConfigView(GovernanceConfig config) {
        return new GovernanceConfigView(
                config.rules().values().stream()
                        .map(this::toGovernanceRuleConfigView)
                        .toList()
        );
    }

    /// A single governance rule as the response body.
    GovernanceRuleConfigView toGovernanceRuleConfigView(GovernanceRuleConfig config) {
        return new GovernanceRuleConfigView(
                config.name(),
                config.errorMessage(),
                config.description(),
                config.selector(),
                config.expression(),
                config.exemptions());
    }

    /// A single rule request as a [GovernanceRuleConfig]. Checks run in a fixed order and fail on
    /// the first problem: the entry and its required fields, then the selector's resource type,
    /// then each expression's language and whether it compiles.
    ///
    /// The rule's name always comes from the path. The body's `name` is optional; when present it
    /// must equal the path name, so a body can never rename or redirect the rule it is sent to.
    ///
    /// @throws IllegalArgumentException if the path name is blank, the request is null, the body
    ///                                  name differs from the path name, the error message is blank,
    ///                                  a missing selector or expression, a resource type other
    ///                                  than `TOPIC`, an expression type other than `CEL`, or a
    ///                                  blank or non-compiling expression
    GovernanceRuleConfig toGovernanceRuleConfig(String name, @Nullable GovernanceRuleRequest req) {
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
        var selectorExpression = req.selector().expression() == null
                ? null
                : expression(name, "selector.expression", req.selector().expression());
        var ruleExpression = expression(name, "expression", req.expression());
        return new GovernanceRuleConfig(
                name,
                req.errorMessage(),
                req.description(),
                new GovernanceRuleConfig.Selector(resourceType, selectorExpression),
                ruleExpression,
                exemptions(name, req.exemptions()));
    }

    /// Checks each exemption in order: not null, a non-blank name unique within the rule, and a
    /// supported, compiling expression. A `null` list means the rule has no exemptions.
    private static List<GovernanceRuleConfig.Exemption> exemptions(
            String rule, @Nullable List<GovernanceRuleRequest.Exemption> raw) {
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
                    expression(rule, "exemption '" + name + "' expression", exemption.expression())));
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
        if (req.expression() == null) {
            throw new IllegalArgumentException("governance rule '" + name + "': expression must not be null");
        }
    }

    /// Only topics are governed today. Matched explicitly: Kafka's `ResourceType.fromString`
    /// maps anything it does not recognise to `UNKNOWN` instead of failing.
    private static ResourceType resourceType(String rule, String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': selector.resourceType must not be blank");
        }
        if (!raw.equalsIgnoreCase(ResourceType.TOPIC.name())) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': unsupported selector.resourceType '" + raw + "'");
        }
        return ResourceType.TOPIC;
    }

    private static GovernanceRuleConfig.Expression expression(
            String rule, String field, GovernanceRuleRequest.Expression raw) {
        if (raw.type() == null || !raw.type().equalsIgnoreCase(GovernanceRuleConfig.Expression.Type.CEL.name())) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': unsupported " + field + ".type '" + raw.type() + "'");
        }
        if (raw.value() == null || raw.value().isBlank()) {
            throw new IllegalArgumentException(
                    "governance rule '" + rule + "': " + field + ".value must not be blank");
        }
        var error = GovernancePolicy.validationError(raw.value());
        if (error.isPresent()) {
            throw new IllegalArgumentException("governance rule '" + rule + "': " + field + ": " + error.get());
        }
        return GovernanceRuleConfig.Expression.cel(raw.value());
    }
}
