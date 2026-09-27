package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.governance.GovernancePolicy;

import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/// Converts between the `/governance` transport types and the [GovernanceConfig] section, and
/// owns the validation the config records used to run from a Jackson-invoked constructor. A
/// malformed regex is reported as the exemption and the field at fault and nothing else, so the
/// parser's diagnostics -- the exception class, the index it failed at, the pattern text -- stay
/// in the gateway; a rule whose CEL expression does not compile carries
/// [GovernancePolicy]'s own message, which quotes the caller's expression.
///
/// Rules are checked before exemptions, and each rule's blank checks precede its CEL check, so a
/// body with both a bad rule and a bad exemption names the rule. Deserializing straight into
/// [GovernanceConfig] used to report the exemption instead: Jackson materialized both maps before
/// the record constructors ran, so every blank and regex check landed before the CEL loop.
final class GovernanceConfigMapper {

    /// The request body as a governance section.
    ///
    /// @throws IllegalArgumentException if a rule entry is null, has a blank message or a blank
    ///                                  or non-compiling CEL expression, or an exemption entry
    ///                                  is null or has a blank or non-compiling regex field
    GovernanceConfig toConfig(GovernanceConfigRequest request) {
        var rules = new HashMap<String, GovernanceRuleConfig>();
        request.topicRules().forEach((name, rule) -> {
            if (rule == null) {
                throw new IllegalArgumentException("governance rule '" + name + "': entry must not be null");
            }
            if (rule.message() == null || rule.message().isBlank()) {
                throw new IllegalArgumentException("governance rule '" + name + "': message must not be blank");
            }
            if (rule.expression() == null || rule.expression().isBlank()) {
                throw new IllegalArgumentException("governance rule '" + name + "': expression must not be blank");
            }
            var error = GovernancePolicy.validationError(rule.expression());
            if (error.isPresent()) {
                throw new IllegalArgumentException("governance rule '" + name + "': " + error.get());
            }
            rules.put(name, new GovernanceRuleConfig(rule.message(), rule.expression()));
        });
        var exemptions = new HashMap<String, GovernanceExemptionConfig>();
        request.exemptions().forEach((name, exemption) -> {
            if (exemption == null) {
                throw new IllegalArgumentException("exemption '" + name + "': entry must not be null");
            }
            requireCompilable(name, "principal", exemption.principal());
            requireCompilable(name, "topicPattern", exemption.topicPattern());
            exemptions.put(name, new GovernanceExemptionConfig(exemption.principal(), exemption.topicPattern()));
        });
        return new GovernanceConfig(rules, exemptions);
    }

    /// The governance section as the response body.
    GovernanceConfigView toView(GovernanceConfig config) {
        return new GovernanceConfigView(config.topicRules(), config.exemptions());
    }

    private static void requireCompilable(String name, String field, String regex) {
        if (regex == null || regex.isBlank()) {
            throw new IllegalArgumentException("exemption '" + name + "': " + field + " must not be blank");
        }
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("exemption '" + name + "': invalid " + field + " regex");
        }
    }
}
