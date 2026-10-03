package io.jonasg.kawa.governance;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelErrorCode;
import dev.cel.common.CelException;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelType;
import dev.cel.common.types.ListType;
import dev.cel.common.types.CelTypeProvider;
import dev.cel.common.types.MapType;
import dev.cel.common.types.StructType;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Match;
import io.jonasg.kawa.config.GovernanceRuleConfig.Operation;
import io.jonasg.kawa.config.GovernanceRuleConfig.SubRule;
import io.jonasg.kawa.config.GovernanceRuleConfig.TopicScope;
import io.jonasg.kawa.config.GovernanceVariableConfig;
import org.apache.kafka.common.resource.ResourceType;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/// Evaluates governance rules against topic, consumer group and transactional-id requests. Rules are CEL expressions compiled
/// eagerly on [reload], so a bad expression fails the config load instead of the first request.
///
/// - A rule's sub-rules compile to the one CEL expression they are equivalent to
///   ([GovernanceRuleConfig#expression()]).
/// - Each rule is compiled against its resource type's context: `topic`, `group` or
///   `transaction`, plus `principal` and `service`.
/// - A rule's selector expression, when present, must be `true` for the rule to apply.
/// - Global exemptions skip every rule; a rule's own exemptions skip that rule alone.
/// - Variables ([GovernanceVariableConfig]) are declared with their CEL type in every
///   environment and bound on every evaluation.
///
/// The compiled snapshot is an immutable object replaced atomically via [reload]: a reader on
/// the hot path sees either the previous or the new snapshot, never a partially-applied one.
public final class GovernancePolicy {

    /// Context variable per governed resource type.
    private static final Map<ResourceType, String> RESOURCE_VARIABLES = Map.of(
            ResourceType.TOPIC, "topic",
            ResourceType.GROUP, "group",
            ResourceType.TRANSACTIONAL_ID, "transaction");
    /// The resource variables are typed structures rather than `map<string, dyn>`: indexing a
    /// `dyn` leaves its type open, and cel-java then binds `int(topic.configs['x'])` to the
    /// `int(int)` overload only, failing at runtime on the string value. Typed fields also turn
    /// `topic.typo` into a compile error. At runtime the values are plain maps; selecting a field
    /// reads the map key, and a missing key is a missing attribute.
    private static final Map<String, StructType> RESOURCE_STRUCTS = Map.of(
            "topic", struct("kawa.Topic", Map.of(
                    "name", SimpleType.STRING,
                    "virtual", SimpleType.BOOL,
                    "partitions", SimpleType.INT,
                    "replicationFactor", SimpleType.INT,
                    "configs", MapType.create(SimpleType.STRING, SimpleType.STRING),
                    "physicalTopic", SimpleType.STRING)),
            "group", struct("kawa.Group", Map.of("id", SimpleType.STRING)),
            "transaction", struct("kawa.Transaction", Map.of("id", SimpleType.STRING)));
    private static final CelTypeProvider RESOURCE_TYPE_PROVIDER = new CelTypeProvider() {
        @Override
        public ImmutableCollection<CelType> types() {
            return ImmutableList.copyOf(RESOURCE_STRUCTS.values());
        }

        @Override
        public Optional<CelType> findType(String typeName) {
            return RESOURCE_STRUCTS.values().stream().filter(t -> t.name().equals(typeName)).map(CelType.class::cast).findFirst();
        }
    };

    /// Names no governance variable may take, because the context already binds them.
    private static final Set<String> RESERVED = Set.of("principal", "service", "topic", "group", "transaction");
    /// Named groups are display hints for a regex variable; RE2 and `java.util.regex` reject a
    /// group name used twice, so they are evaluated as plain groups.
    private static final Pattern NAMED_GROUP = Pattern.compile("\\(\\?<(?![=!])[A-Za-z0-9_]+>");
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final CelRuntime RUNTIME = CelRuntimeFactory.plannerRuntimeBuilder().build();
    /// Environments without variables, for validating a lone expression.
    private static final Environments NO_VARIABLES = environments(List.of());

    /// Bound on [Snapshot#decisions]; the cache is cleared when it fills up.
    private static final int MAX_CACHED_DECISIONS = 10_000;

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

    /// Evaluates all rules against a physical topic creation request. Returns every violation,
    /// sorted by rule name; an empty list means the topic is compliant.
    ///
    /// No rule applies when a global exemption evaluates to `true`. A rule is skipped when its
    /// selector expression is not `true` or one of its own exemptions evaluates to `true`; other
    /// rules still apply. An exemption that throws or does not return `true` does not apply, so
    /// the rule is enforced. Only topic rules covering physical topics apply.
    ///
    /// Fail-closed: a rule that throws during evaluation raises [IllegalStateException] so the
    /// request is rejected, and a rule that does not return `true` counts as a violation.
    public List<Violation> evaluate(String principal, String service, TopicSpec topic) {
        return evaluate(GovernanceRequest.topic(principal, service, topic));
    }

    /// Evaluates every rule that applies to `request` and returns its violations, sorted by
    /// rule name; empty means the request is compliant. Fail-closed like
    /// [#evaluate(String, String, TopicSpec)].
    ///
    /// Group and transactional-id verdicts are cached per request until the next [#reload]; a
    /// rule that fails to evaluate is not cached, so it is retried.
    public List<Violation> evaluate(GovernanceRequest request) {
        Snapshot current = snapshot;
        if (request.resourceType() == ResourceType.TOPIC) {
            return violations(dryRun(current, request));
        }
        List<Violation> cached = current.decisions().get(request);
        if (cached != null) {
            return cached;
        }
        List<Violation> violations = violations(dryRun(current, request));
        if (current.decisions().size() >= MAX_CACHED_DECISIONS) {
            current.decisions().clear();
        }
        current.decisions().put(request, violations);
        return violations;
    }

    private static List<Violation> violations(GovernanceTrace trace) {
        List<Violation> violations = new ArrayList<>();
        for (GovernanceTrace.RuleTrace rule : trace.rules()) {
            if (rule.outcome() == GovernanceTrace.Outcome.ERROR) {
                throw new IllegalStateException(
                        "Failed to evaluate governance rule '" + rule.rule() + "': " + rule.detail());
            }
            if (rule.outcome() == GovernanceTrace.Outcome.FAIL) {
                violations.add(new Violation(rule.rule(), rule.message(), rule.path()));
            }
        }
        violations.sort(Comparator.comparing(Violation::rule));
        return List.copyOf(violations);
    }

    /// Evaluates every stored rule for the request's resource type and topic kind, without
    /// side effects, and reports each rule, sub-rule and check.
    public GovernanceTrace dryRun(GovernanceRequest request) {
        return dryRun(snapshot, request);
    }

    private static GovernanceTrace dryRun(Snapshot current, GovernanceRequest request) {
        Map<String, Object> bindings = bindings(current, request);
        for (CompiledExemption exemption : current.exemptions()) {
            if (holds(exemption.program(), bindings)) {
                return new GovernanceTrace(exemption.name(), List.of());
            }
        }
        List<GovernanceTrace.RuleTrace> rules = new ArrayList<>();
        for (CompiledRule rule : current.rules()) {
            if (applies(rule, request)) {
                rules.add(trace(rule, bindings));
            }
        }
        rules.sort(Comparator.comparing(GovernanceTrace.RuleTrace::rule));
        return new GovernanceTrace(null, rules);
    }

    /// Evaluates one rule that is not stored, e.g. while it is being authored, against the
    /// stored variables. Global exemptions are not consulted, so the answer is about the rule
    /// alone.
    ///
    /// @throws IllegalArgumentException if the rule does not compile
    public GovernanceTrace dryRun(GovernanceRequest request, GovernanceRuleConfig rule) {
        Snapshot current = snapshot;
        CompiledRule compiled = compileRule(current.environments(), rule.name(), rule);
        List<GovernanceTrace.RuleTrace> rules = applies(compiled, request)
                ? List.of(trace(compiled, bindings(current, request)))
                : List.of();
        return new GovernanceTrace(null, rules);
    }

    /// Whether any stored rule runs on physical topics for `operation`; when none does, the
    /// gateway skips reading a topic's current state for a change.
    public boolean hasPhysicalTopicRules(Operation operation) {
        return snapshot.rules().stream().anyMatch(rule -> rule.resourceType() == ResourceType.TOPIC
                && rule.scope() != TopicScope.VIRTUAL
                && rule.operations().contains(operation));
    }

    /// Operations and scope only narrow topic rules: groups and transactional ids are judged
    /// whenever a client uses them.
    private static boolean applies(CompiledRule rule, GovernanceRequest request) {
        if (rule.resourceType() != request.resourceType()) {
            return false;
        }
        if (rule.resourceType() != ResourceType.TOPIC) {
            return true;
        }
        if (!rule.operations().contains(request.operation())) {
            return false;
        }
        return rule.scope() == TopicScope.BOTH
                || rule.scope() == (request.virtual() ? TopicScope.VIRTUAL : TopicScope.PHYSICAL);
    }

    private static Map<String, Object> bindings(Snapshot current, GovernanceRequest request) {
        Map<String, Object> bindings = new HashMap<>(current.variables());
        bindings.put("principal", request.principal());
        bindings.put("service", request.service());
        Map<String, Object> resource = new HashMap<>();
        request.resource().forEach((k, v) -> resource.put(k, v instanceof Integer i ? Long.valueOf(i) : v));
        if (request.resourceType() == ResourceType.TOPIC) {
            resource.put("virtual", request.virtual());
            if (resource.get("configs") instanceof Map<?, ?> configs) {
                Map<String, String> strings = new HashMap<>();
                configs.forEach((k, v) -> strings.put(String.valueOf(k), String.valueOf(v)));
                resource.put("configs", strings);
            }
        }
        bindings.put(RESOURCE_VARIABLES.get(request.resourceType()), resource);
        return bindings;
    }

    /// Selector, then the rule's exemptions, then its sub-rules.
    private static GovernanceTrace.RuleTrace trace(CompiledRule rule, Map<String, Object> bindings) {
        if (rule.selector() != null) {
            try {
                if (!Boolean.TRUE.equals(rule.selector().eval(bindings))) {
                    return new GovernanceTrace.RuleTrace(rule.name(), GovernanceTrace.Outcome.SKIPPED, null, null, null, null, null);
                }
            } catch (CelEvaluationException e) {
                if (missingAttribute(e)) {
                    // A selector reading something the request lacks does not select it.
                    return new GovernanceTrace.RuleTrace(rule.name(), GovernanceTrace.Outcome.SKIPPED, null, null, null, null, null);
                }
                return new GovernanceTrace.RuleTrace(rule.name(), GovernanceTrace.Outcome.ERROR,
                        "selector: " + e.getMessage(), null, null, null, null);
            }
        }
        for (CompiledExemption exemption : rule.exemptions()) {
            if (holds(exemption.program(), bindings)) {
                return new GovernanceTrace.RuleTrace(rule.name(), GovernanceTrace.Outcome.EXEMPTED, null, exemption.name(), null, null, null);
            }
        }
        Combined result = combine(rule.match(), rule.subRules(), bindings);
        if (!result.outcome().refuses()) {
            return new GovernanceTrace.RuleTrace(rule.name(), result.outcome(), null, null, null, null, result.children());
        }
        Explanation why = explain(rule.match(), rule.subRules(), result.children(), rule.message());
        return new GovernanceTrace.RuleTrace(rule.name(), result.outcome(), result.detail(), null, why.path(), why.message(),
                result.children());
    }

    private record Combined(GovernanceTrace.Outcome outcome, String detail, List<GovernanceTrace.NodeTrace> children) {
    }

    /// Evaluates sub-rules left to right with `&&` / `||` short-circuiting: `ALL` stops at the
    /// first that does not pass, `ANY` at the first that passes; the rest are skipped.
    private static Combined combine(Match match, List<? extends CompiledNode> nodes, Map<String, Object> bindings) {
        List<GovernanceTrace.NodeTrace> children = new ArrayList<>();
        GovernanceTrace.NodeTrace decided = null;
        for (CompiledNode node : nodes) {
            if (decided != null) {
                children.add(skipped(node));
                continue;
            }
            GovernanceTrace.NodeTrace child = evaluate(node, bindings);
            children.add(child);
            boolean decisive = match == Match.ALL
                    ? child.outcome() != GovernanceTrace.Outcome.PASS
                    : child.outcome() == GovernanceTrace.Outcome.PASS;
            if (decisive) {
                decided = child;
            }
        }
        if (match == Match.ALL) {
            return decided == null
                    ? new Combined(GovernanceTrace.Outcome.PASS, null, children)
                    : new Combined(decided.outcome(), decided.detail(), children);
        }
        if (decided != null) {
            return new Combined(GovernanceTrace.Outcome.PASS, null, children);
        }
        return children.stream()
                .filter(c -> c.outcome() == GovernanceTrace.Outcome.ERROR)
                .findFirst()
                .map(c -> new Combined(GovernanceTrace.Outcome.ERROR, c.detail(), children))
                .orElseGet(() -> new Combined(GovernanceTrace.Outcome.FAIL, null, children));
    }

    private static GovernanceTrace.NodeTrace evaluate(CompiledNode node, Map<String, Object> bindings) {
        return switch (node) {
            case CompiledCheck check -> {
                try {
                    boolean holds = Boolean.TRUE.equals(check.program().eval(bindings));
                    yield new GovernanceTrace.NodeTrace(check.name(),
                            holds ? GovernanceTrace.Outcome.PASS : GovernanceTrace.Outcome.FAIL, null, null);
                } catch (CelEvaluationException e) {
                    // Reading what the request lacks, e.g. topic.configs['retention.ms'] when it is
                    // not set, fails the check like a false result; anything else is an error.
                    yield missingAttribute(e)
                            ? new GovernanceTrace.NodeTrace(check.name(), GovernanceTrace.Outcome.FAIL, null, null)
                            : new GovernanceTrace.NodeTrace(check.name(), GovernanceTrace.Outcome.ERROR, e.getMessage(), null);
                }
            }
            case CompiledGroup group -> {
                Combined result = combine(group.match(), group.checks(), bindings);
                yield new GovernanceTrace.NodeTrace(group.name(), result.outcome(), result.detail(), result.children());
            }
        };
    }

    private static GovernanceTrace.NodeTrace skipped(CompiledNode node) {
        List<GovernanceTrace.NodeTrace> checks = node instanceof CompiledGroup group
                ? group.checks().stream().map(GovernancePolicy::skipped).toList()
                : List.of();
        return new GovernanceTrace.NodeTrace(node.name(), GovernanceTrace.Outcome.SKIPPED, null, checks);
    }

    private record Explanation(List<String> path, String message) {
    }

    /// Follows the decisive failure down the tree. Under `ALL` that is the failing child, whose
    /// own message wins over the inherited one; under `ANY` every branch failed, so the level
    /// itself is the most specific answer.
    private static Explanation explain(
            Match match, List<? extends CompiledNode> nodes, List<GovernanceTrace.NodeTrace> children, String inherited) {
        if (match == Match.ANY) {
            return new Explanation(List.of(), inherited);
        }
        for (int i = 0; i < children.size(); i++) {
            if (!children.get(i).outcome().refuses()) {
                continue;
            }
            CompiledNode node = nodes.get(i);
            String own = node.errorMessage() == null || node.errorMessage().isBlank() ? inherited : node.errorMessage();
            if (node instanceof CompiledGroup group) {
                Explanation deeper = explain(group.match(), group.checks(), children.get(i).checks(), own);
                List<String> path = new ArrayList<>();
                path.add(node.name());
                path.addAll(deeper.path());
                return new Explanation(List.copyOf(path), deeper.message());
            }
            return new Explanation(List.of(node.name()), own);
        }
        return new Explanation(List.of(), inherited);
    }

    /// Whether the evaluation failed because the expression read a map key or field the request
    /// does not have, such as a config that is not set.
    private static boolean missingAttribute(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t.getClass().getSimpleName().equals("CelAttributeNotFoundException")) {
                return true;
            }
            if (t instanceof CelException cel && cel.getErrorCode() == CelErrorCode.ATTRIBUTE_NOT_FOUND) {
                return true;
            }
        }
        return false;
    }

    /// Whether `program` evaluates to `true`. An exemption that throws is treated as not
    /// applying, so a broken exemption can never switch a rule off.
    private static boolean holds(CelRuntime.Program program, Map<String, Object> bindings) {
        try {
            return Boolean.TRUE.equals(program.eval(bindings));
        } catch (CelEvaluationException e) {
            return false;
        }
    }

    /// Validates a single CEL expression against the topic context, for use when a rule is
    /// being authored (e.g. via the admin API). Returns the validation error, or empty when valid.
    public static Optional<String> validationError(String expression) {
        return validationError(ResourceType.TOPIC, expression);
    }

    /// Validates a single CEL expression against `resourceType`'s context, without variables.
    /// Returns the validation error, or empty when valid.
    ///
    /// @throws IllegalArgumentException if `resourceType` is not governed
    public static Optional<String> validationError(ResourceType resourceType, String expression) {
        return validationError(NO_VARIABLES.forResource(resourceType), expression);
    }

    /// Validates a single CEL expression against `resourceType`'s context and `variables`.
    /// Returns the validation error, or empty when valid.
    ///
    /// @throws IllegalArgumentException if `resourceType` is not governed or a variable is invalid
    public static Optional<String> validationError(
            ResourceType resourceType, String expression, Collection<GovernanceVariableConfig> variables) {
        return validationError(environments(variables).forResource(resourceType), expression);
    }

    /// Validates a global exemption expression, which may read any resource variable.
    public static Optional<String> globalExemptionValidationError(String expression) {
        return validationError(NO_VARIABLES.global(), expression);
    }

    /// Validates a global exemption expression against `variables` as well.
    public static Optional<String> globalExemptionValidationError(String expression, Collection<GovernanceVariableConfig> variables) {
        return validationError(environments(variables).global(), expression);
    }

    /// Whether rules for `resourceType` can be authored.
    public static boolean governs(ResourceType resourceType) {
        return RESOURCE_VARIABLES.containsKey(resourceType);
    }

    /// Compiles `config` without keeping it, so a write can be refused before it is persisted:
    /// e.g. removing a variable a rule still reads, or changing its type under it.
    ///
    /// @throws IllegalArgumentException naming the first rule, exemption or variable at fault
    public static void validate(GovernanceConfig config) {
        compile(config);
    }

    /// The Java value `variable` binds: `String`, `Long`, `Double`, `Boolean`, `List<String>`
    /// or `List<Long>`. A string's named groups become plain groups.
    ///
    /// @throws IllegalArgumentException if the value is not valid JSON of the declared type
    public static Object variableValue(GovernanceVariableConfig variable) {
        Object raw;
        try {
            raw = JSON.readValue(variable.value(), Object.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("governance variable '" + variable.name()
                    + "': value is not a valid literal; strings take double quotes and lists use [ ]");
        }
        var type = variable.type();
        Object value = switch (type) {
            case STRING -> raw instanceof String text ? NAMED_GROUP.matcher(text).replaceAll("(?:") : null;
            case INT -> isIntegral(raw) ? ((Number) raw).longValue() : null;
            case DOUBLE -> raw instanceof Number number ? number.doubleValue() : null;
            case BOOL -> raw instanceof Boolean bool ? bool : null;
            case LIST_STRING -> raw instanceof List<?> list && list.stream().allMatch(String.class::isInstance)
                    ? List.copyOf(list) : null;
            case LIST_INT -> raw instanceof List<?> list && list.stream().allMatch(GovernancePolicy::isIntegral)
                    ? list.stream().map(n -> ((Number) n).longValue()).toList() : null;
        };
        if (value == null) {
            throw new IllegalArgumentException(
                    "governance variable '" + variable.name() + "': value is not a " + type.celName());
        }
        return value;
    }

    private static boolean isIntegral(Object value) {
        return value instanceof Integer || value instanceof Long || value instanceof BigInteger;
    }

    private static Optional<String> validationError(CelCompiler compiler, String expression) {
        try {
            compileExpression(compiler, expression);
            return Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.of(e.getMessage());
        }
    }

    private static Snapshot compile(GovernanceConfig config) {
        Environments environments = environments(config.variables().values());
        Map<String, Object> variables = new HashMap<>();
        for (GovernanceVariableConfig variable : config.variables().values()) {
            variables.put(variable.name(), variableValue(variable));
        }
        List<CompiledExemption> globalExemptions = new ArrayList<>();
        for (GovernanceRuleConfig.Exemption exemption : config.exemptions().values()) {
            try {
                globalExemptions.add(new CompiledExemption(
                        exemption.name(), compileExpression(environments.global(), exemption.expression().value())));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid global governance exemption '" + exemption.name() + "': " + e.getMessage(), e);
            }
        }
        List<CompiledRule> rules = new ArrayList<>();
        for (Map.Entry<String, GovernanceRuleConfig> entry : config.rules().entrySet()) {
            rules.add(compileRule(environments, entry.getKey(), entry.getValue()));
        }
        return new Snapshot(List.copyOf(rules), List.copyOf(globalExemptions), Map.copyOf(variables), environments);
    }

    /// Compiles the rule's selector, exemptions and every check on its own, so a trace can
    /// say which check failed.
    private static CompiledRule compileRule(Environments environments, String ruleName, GovernanceRuleConfig rule) {
        ResourceType resourceType = rule.selector() == null ? ResourceType.TOPIC : rule.selector().resourceType();
        CelCompiler compiler = environments.forRule(ruleName, resourceType);
        List<CompiledExemption> exemptions = new ArrayList<>();
        for (GovernanceRuleConfig.Exemption exemption : rule.exemptions()) {
            exemptions.add(new CompiledExemption(exemption.name(), compile(compiler, ruleName,
                    "exemption '" + exemption.name() + "': ", exemption.expression().value())));
        }
        List<CompiledNode> subRules = new ArrayList<>();
        for (SubRule subRule : rule.subRules()) {
            subRules.add(switch (subRule) {
                case SubRule.Check check -> compileCheck(compiler, ruleName, check);
                case SubRule.Group group -> new CompiledGroup(group.name(), group.errorMessage(), group.match(),
                        group.checks().stream().map(check -> compileCheck(compiler, ruleName, check)).toList());
            });
        }
        var selectorExpression = rule.selector() == null ? null : rule.selector().expression();
        return new CompiledRule(
                ruleName,
                rule.errorMessage(),
                resourceType,
                rule.selector() == null ? TopicScope.BOTH : rule.selector().scope(),
                rule.selector() == null ? Set.of(Operation.CREATE) : rule.selector().operations(),
                selectorExpression == null ? null : compile(compiler, ruleName, "selector: ", selectorExpression.value()),
                rule.match(),
                List.copyOf(subRules),
                List.copyOf(exemptions));
    }

    private static CompiledCheck compileCheck(CelCompiler compiler, String ruleName, SubRule.Check check) {
        String what = ruleName.equals(check.name()) ? "" : "sub-rule '" + check.name() + "': ";
        return new CompiledCheck(check.name(), check.errorMessage(), compile(compiler, ruleName, what, check.expression().value()));
    }

    /// One compiler per governed resource type, plus the global one, all declaring `variables`.
    private static Environments environments(Collection<GovernanceVariableConfig> variables) {
        for (GovernanceVariableConfig variable : variables) {
            if (RESERVED.contains(variable.name())) {
                throw new IllegalArgumentException(
                        "governance variable '" + variable.name() + "': the name is reserved for the request context");
            }
        }
        Map<ResourceType, CelCompiler> perResource = new HashMap<>();
        RESOURCE_VARIABLES.forEach((type, name) -> perResource.put(type, compiler(List.of(name), variables)));
        return new Environments(Map.copyOf(perResource), compiler(List.copyOf(RESOURCE_VARIABLES.values()), variables));
    }

    private static CelCompiler compiler(List<String> resourceVariables, Collection<GovernanceVariableConfig> variables) {
        // Standard macros (has, all, exists, exists_one, map, filter) are opt-in in cel-java;
        // without them `list.exists(x, …)` fails as an undeclared reference.
        var builder = CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addVar("principal", SimpleType.STRING)
                .addVar("service", SimpleType.STRING);
        builder.setTypeProvider(RESOURCE_TYPE_PROVIDER);
        for (String variable : resourceVariables) {
            builder.addVar(variable, RESOURCE_STRUCTS.get(variable));
        }
        for (GovernanceVariableConfig variable : variables) {
            builder.addVar(variable.name(), celType(variable.type()));
        }
        return builder.build();
    }

    private static StructType struct(String name, Map<String, CelType> fields) {
        return StructType.create(name, ImmutableSet.copyOf(fields.keySet()), field -> Optional.ofNullable(fields.get(field)));
    }

    private static CelType celType(GovernanceVariableConfig.Type type) {
        return switch (type) {
            case STRING -> SimpleType.STRING;
            case INT -> SimpleType.INT;
            case DOUBLE -> SimpleType.DOUBLE;
            case BOOL -> SimpleType.BOOL;
            case LIST_STRING -> ListType.create(SimpleType.STRING);
            case LIST_INT -> ListType.create(SimpleType.INT);
        };
    }

    /// @param perResource compiler per governed resource type
    /// @param global      compiler declaring every resource variable, for global exemptions
    private record Environments(Map<ResourceType, CelCompiler> perResource, CelCompiler global) {

        CelCompiler forResource(ResourceType resourceType) {
            CelCompiler compiler = perResource.get(resourceType);
            if (compiler == null) {
                throw new IllegalArgumentException("Unsupported governance resource type '" + resourceType + "'");
            }
            return compiler;
        }

        CelCompiler forRule(String ruleName, ResourceType resourceType) {
            try {
                return forResource(resourceType);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid governance rule '" + ruleName + "': " + e.getMessage(), e);
            }
        }
    }

    private static CelRuntime.Program compile(CelCompiler compiler, String ruleName, String what, String expression) {
        try {
            return compileExpression(compiler, expression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid governance rule '" + ruleName + "': " + what + e.getMessage(), e);
        }
    }

    private static CelRuntime.Program compileExpression(CelCompiler compiler, String expression) {
        try {
            CelAbstractSyntaxTree ast = compiler.compile(expression).getAst();
            return RUNTIME.createProgram(ast);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException(
                    "Invalid CEL expression '" + expression + "': " + e.getMessage(), e);
        }
    }

    /// @param variables    the value bound for each variable name
    /// @param environments the compilers, kept to compile unsaved rules for a dry run
    /// @param decisions    group and transactional-id verdicts, which clients ask for on every
    ///                     heartbeat and commit; it lives and dies with the snapshot, so a reload
    ///                     starts empty
    private record Snapshot(
            List<CompiledRule> rules,
            List<CompiledExemption> exemptions,
            Map<String, Object> variables,
            Environments environments,
            Map<GovernanceRequest, List<Violation>> decisions
    ) {

        Snapshot(List<CompiledRule> rules, List<CompiledExemption> exemptions, Map<String, Object> variables, Environments environments) {
            this(rules, exemptions, variables, environments, new ConcurrentHashMap<>());
        }
    }

    private record CompiledExemption(String name, CelRuntime.Program program) {
    }

    /// @param selector the compiled selector expression, or `null` when the rule selects every resource
    private record CompiledRule(
            String name,
            String message,
            ResourceType resourceType,
            TopicScope scope,
            Set<Operation> operations,
            CelRuntime.Program selector,
            Match match,
            List<CompiledNode> subRules,
            List<CompiledExemption> exemptions
    ) {
    }

    private sealed interface CompiledNode permits CompiledCheck, CompiledGroup {
        String name();

        String errorMessage();
    }

    private record CompiledCheck(String name, String errorMessage, CelRuntime.Program program) implements CompiledNode {
    }

    private record CompiledGroup(String name, String errorMessage, Match match, List<CompiledCheck> checks) implements CompiledNode {
    }
}
