package io.jonasg.kawa.governance;

import java.util.List;

/// What governance decided for one request, rule by rule and check by check: the dry-run
/// answer, and what [GovernancePolicy#evaluate] turns into [Violation]s.
///
/// @param exemptedBy  the global exemption that matched, or `null`; when set, no rule ran
/// @param rules       one trace per rule for the request's resource type and topic kind
public record GovernanceTrace(String exemptedBy, List<RuleTrace> rules) {

    public GovernanceTrace {
        rules = List.copyOf(rules);
    }

    /// Whether the request passes: globally exempted, or no rule failed or errored.
    public boolean allowed() {
        return exemptedBy != null || rules.stream().noneMatch(r -> r.outcome().refuses());
    }

    /// How one rule, sub-rule or check ended.
    public enum Outcome {
        /// Held.
        PASS,
        /// Did not hold.
        FAIL,
        /// Failed to evaluate; refuses the request (fail-closed).
        ERROR,
        /// Not evaluated: a rule whose selector is false, or a sub-rule an earlier sibling
        /// already decided (`ALL` after a failure, `ANY` after a pass).
        SKIPPED,
        /// A rule one of its own exemptions matched.
        EXEMPTED;

        public boolean refuses() {
            return this == FAIL || this == ERROR;
        }
    }

    /// @param rule       the rule name
    /// @param outcome    how the rule ended
    /// @param detail     the evaluation error, for [Outcome#ERROR]
    /// @param exemptedBy the rule exemption that matched, for [Outcome#EXEMPTED]
    /// @param path       on failure, the sub-rule names leading to the decisive failure
    /// @param message    on failure, the most specific error message along [path]
    /// @param subRules   one trace per sub-rule, in order
    public record RuleTrace(
            String rule,
            Outcome outcome,
            String detail,
            String exemptedBy,
            List<String> path,
            String message,
            List<NodeTrace> subRules
    ) {
        public RuleTrace {
            path = path == null ? List.of() : List.copyOf(path);
            subRules = subRules == null ? List.of() : List.copyOf(subRules);
        }
    }

    /// @param name    the sub-rule or check name
    /// @param outcome how it ended
    /// @param detail  the evaluation error, for [Outcome#ERROR]
    /// @param checks  a group's checks; empty for a check
    public record NodeTrace(String name, Outcome outcome, String detail, List<NodeTrace> checks) {
        public NodeTrace {
            checks = checks == null ? List.of() : List.copyOf(checks);
        }
    }
}
