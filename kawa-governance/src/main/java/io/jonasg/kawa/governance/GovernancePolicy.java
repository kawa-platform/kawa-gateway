package io.jonasg.kawa.governance;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Evaluates topic governance rules against new topic requests. Rules are CEL expressions
/// compiled eagerly on [reload], so a bad expression fails the config load instead of the
/// first request. Each rule carries its own exemptions, compiled alongside it: when any of a
/// rule's exemptions evaluates to `true` for a request, that rule alone is skipped.
///
/// The compiled snapshot is an immutable object replaced atomically via [reload]: a reader on
/// the hot path sees either the previous or the new snapshot, never a partially-applied one.
public final class GovernancePolicy {

    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .addVar("principal", SimpleType.STRING)
            .addVar("service", SimpleType.STRING)
            .addVar("topic", MapType.create(SimpleType.STRING, SimpleType.DYN))
            .build();
    private static final CelRuntime RUNTIME = CelRuntimeFactory.plannerRuntimeBuilder().build();

    private volatile Snapshot snapshot;

    public GovernancePolicy(GovernanceConfig config) {
        reload(config);
    }

    /// Replaces the compiled rule snapshot with one built from `config`.
    ///
    /// Safe to call concurrently with readers: the new snapshot is assigned to a single
    /// `volatile` reference, so readers see either the previous or the new snapshot, never a
    /// partially-applied one. Throws [IllegalArgumentException] if any rule expression fails
    /// to compile; the previous snapshot is left untouched in that case.
    public void reload(GovernanceConfig config) {
        this.snapshot = compile(config);
    }

    /// Evaluates all rules against a topic creation request. Returns every violation, sorted
    /// by rule name; an empty list means the topic is compliant.
    ///
    /// A rule is skipped when one of its exemptions evaluates to `true`; other rules still apply.
    /// An exemption that throws or does not return `true` does not apply, so the rule is enforced.
    ///
    /// Fail-closed: a rule that throws during evaluation raises [IllegalStateException] so the
    /// request is rejected, and a rule that does not return `true` counts as a violation.
    public List<Violation> evaluate(String principal, String service, TopicSpec topic) {
        Snapshot current = snapshot;
        Map<String, Object> bindings = new HashMap<>(3);
        bindings.put("principal", principal);
        bindings.put("service", service);
        Map<String, Object> topicBindings = new HashMap<>(4);
        topicBindings.put("name", topic.name());
        topicBindings.put("partitions", topic.partitions());
        topicBindings.put("replicationFactor", topic.replicationFactor());
        topicBindings.put("configs", new DefaultingMap(topic.configs()));
        bindings.put("topic", topicBindings);

        List<Violation> violations = new ArrayList<>();
        for (CompiledRule rule : current.rules()) {
            if (exempted(rule, bindings)) {
                continue;
            }
            try {
                Object result = rule.program().eval(bindings);
                if (!Boolean.TRUE.equals(result)) {
                    violations.add(new Violation(rule.name(), rule.message()));
                }
            } catch (CelEvaluationException e) {
                throw new IllegalStateException(
                        "Failed to evaluate governance rule '" + rule.name() + "': " + e.getMessage(), e);
            }
        }
        violations.sort(Comparator.comparing(Violation::rule));
        return List.copyOf(violations);
    }

    /// Whether any of `rule`'s exemptions evaluates to `true`. An exemption that throws is treated
    /// as not applying, so a broken exemption can never switch a rule off.
    private static boolean exempted(CompiledRule rule, Map<String, Object> bindings) {
        for (CelRuntime.Program exemption : rule.exemptions()) {
            try {
                if (Boolean.TRUE.equals(exemption.eval(bindings))) {
                    return true;
                }
            } catch (CelEvaluationException e) {
                // does not apply: the rule stays enforced
            }
        }
        return false;
    }

    /// Validates a single CEL expression without a rule name, for use when a rule is being
    /// authored (e.g. via the admin API). Returns the validation error, or empty when valid.
    public static Optional<String> validationError(String expression) {
        try {
            compileExpression(expression);
            return Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.of(e.getMessage());
        }
    }

    private static Snapshot compile(GovernanceConfig config) {
        List<CompiledRule> rules = new ArrayList<>();
        for (Map.Entry<String, GovernanceRuleConfig> entry : config.rules().entrySet()) {
            String ruleName = entry.getKey();
            GovernanceRuleConfig rule = entry.getValue();
            List<CelRuntime.Program> exemptions = new ArrayList<>();
            for (GovernanceRuleConfig.Exemption exemption : rule.exemptions()) {
                exemptions.add(compile(ruleName, "exemption '" + exemption.name() + "': ",
                        exemption.expression().value()));
            }
            rules.add(new CompiledRule(
                    ruleName,
                    rule.errorMessage(),
                    compile(ruleName, "", rule.expression().value()),
                    List.copyOf(exemptions)));
        }
        return new Snapshot(List.copyOf(rules));
    }

    private static CelRuntime.Program compile(String ruleName, String what, String expression) {
        try {
            return compileExpression(expression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid governance rule '" + ruleName + "': " + what + e.getMessage(), e);
        }
    }

    private static CelRuntime.Program compileExpression(String expression) {
        try {
            CelAbstractSyntaxTree ast = COMPILER.compile(expression).getAst();
            return RUNTIME.createProgram(ast);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException(
                    "Invalid CEL expression '" + expression + "': " + e.getMessage(), e);
        }
    }

    private record Snapshot(List<CompiledRule> rules) {
    }

    private record CompiledRule(
            String name,
            String message,
            CelRuntime.Program program,
            List<CelRuntime.Program> exemptions
    ) {
    }

    /// A map that returns `""` for absent keys, so `topic.configs['key']` in CEL expressions is
    /// falsy instead of erroring when a config is missing. Presence can still be tested with the
    /// `in` operator, which relies on `containsKey` and is unaffected by this override.
    private static final class DefaultingMap extends HashMap<String, String> {

        DefaultingMap(Map<String, String> configs) {
            super(configs);
        }

        @Override
        public String get(Object key) {
            return containsKey(key) ? super.get(key) : "";
        }
    }
}
