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
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/// Evaluates topic governance rules against new topic requests. Rules are CEL expressions
/// compiled eagerly on [reload], so a bad expression fails the config load instead of the
/// first request. Exemptions skip evaluation for matching principal + topic pairs.
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
        for (Map.Entry<String, CelRuntime.Program> entry : current.programs().entrySet()) {
            try {
                Object result = entry.getValue().eval(bindings);
                if (!Boolean.TRUE.equals(result)) {
                    violations.add(new Violation(entry.getKey(), current.messages().get(entry.getKey())));
                }
            } catch (CelEvaluationException e) {
                throw new IllegalStateException(
                        "Failed to evaluate governance rule '" + entry.getKey() + "': " + e.getMessage(), e);
            }
        }
        violations.sort(Comparator.comparing(Violation::rule));
        return List.copyOf(violations);
    }

    /// Whether the principal + topic pair is exempt from governance evaluation. Both the
    /// principal and the topic pattern must match for an exemption to apply.
    public boolean exempt(String principal, String topicName) {
        Snapshot current = snapshot;
        for (GovernanceExemptionConfig exemption : current.exemptions().values()) {
            if (Pattern.matches(exemption.principal(), principal)
                && Pattern.matches(exemption.topicPattern(), topicName)) {
                return true;
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
        Map<String, CelRuntime.Program> programs = new HashMap<>();
        Map<String, String> messages = new HashMap<>();
        for (Map.Entry<String, GovernanceRuleConfig> entry : config.rules().entrySet()) {
            programs.put(entry.getKey(), compile(entry.getKey(), entry.getValue().expression().value()));
            messages.put(entry.getKey(), entry.getValue().errorMessage());
        }
        return new Snapshot(Map.copyOf(programs), Map.copyOf(messages), config.exemptions());
    }

    private static CelRuntime.Program compile(String ruleName, String expression) {
        try {
            return compileExpression(expression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid governance rule '" + ruleName + "': " + e.getMessage(), e);
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

    private record Snapshot(
            Map<String, CelRuntime.Program> programs,
            Map<String, String> messages,
            Map<String, GovernanceExemptionConfig> exemptions
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
