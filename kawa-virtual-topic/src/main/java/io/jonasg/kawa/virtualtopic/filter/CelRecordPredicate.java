package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelOptions;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.jonasg.kawa.config.CelFilterConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CelRecordPredicate implements RecordPredicate {

    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .addVar("key", SimpleType.STRING)
            .addVar("value", SimpleType.STRING)
            .addVar("headers", MapType.create(SimpleType.STRING, SimpleType.STRING))
            .addVar("timestamp", SimpleType.INT)
            .build();

    /// Heterogeneous numeric comparisons let `value.amount > 100` work whether the JSON number
    /// decoded to a `Long` or a `Double`.
    private static final CelOptions OPTIONS = CelOptions.current()
            .enableHeterogeneousNumericComparisons(true)
            .build();

    /// Without a value format, `value` is the raw payload as a string.
    private static final CelCompiler RAW_VALUE_COMPILER = compiler(SimpleType.STRING);

    /// With a value format, `value` is the decoded document (dyn can be a map, list or scalar).
    private static final CelCompiler DECODED_VALUE_COMPILER = compiler(SimpleType.DYN);

    private static final CelRuntime RUNTIME = CelRuntimeFactory.plannerRuntimeBuilder()
            .setOptions(OPTIONS)
            .build();
    /// Compiled CEL programs keyed by expression string. Compilation is expensive and the same
    /// expression is reused for every record of a virtual topic, so it is done once and cached.
    /// `CelRuntime.Program` is thread-safe and side-effect free, so a single instance is shared.
    private final Map<String, CelRuntime.Program> celPrograms = new ConcurrentHashMap<>();

    /// The compiled expression. Compilation is expensive, so it happens once here - which also
    /// rejects an invalid expression when the filter is created rather than on the first record.
    /// `CelRuntime.Program` is thread-safe and side-effect free, so the instance can be shared.
    private final CelRuntime.Program program;
    private final String expression;
    private final PayloadDecoder valueDecoder;

    /// @param valueDecoder decodes the record value before evaluation, or `null` to bind the
    ///                     raw value as a string
    public CelRecordPredicate(CelFilterConfig config, PayloadDecoder valueDecoder) {
        this.expression = config.expression();
        this.valueDecoder = valueDecoder;
        this.program = compile(valueDecoder == null ? RAW_VALUE_COMPILER : DECODED_VALUE_COMPILER, expression);
    }

    @Override
    public boolean test(Record record) {
        Map<String, Object> bindings = new HashMap<>(4);
        bindings.put("key", decode(record.key()));
        bindings.put("value", valueDecoder == null ? decode(record.value()) : valueDecoder.decode(record.value()));
        bindings.put("headers", headers(record));
        bindings.put("timestamp", record.timestamp());
        try {
            return Boolean.TRUE.equals(program.eval(bindings));
        } catch (CelEvaluationException e) {
            throw new IllegalStateException(
                    "Failed to evaluate CEL filter '" + expression + "': " + e.getMessage(), e);
        }
    }

    private static CelCompiler compiler(CelType valueType) {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setOptions(OPTIONS)
                .setStandardMacros(CelStandardMacro.HAS)
                .addVar("key", SimpleType.STRING)
                .addVar("value", valueType)
                .addVar("headers", MapType.create(SimpleType.STRING, SimpleType.STRING))
                .addVar("timestamp", SimpleType.INT)
                .build();
    }

    private static CelRuntime.Program compile(CelCompiler compiler, String expression) {
        try {
            CelAbstractSyntaxTree ast = compiler.compile(expression).getAst();
            return RUNTIME.createProgram(ast);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException(
                    "Invalid CEL filter expression '" + expression + "': " + e.getMessage(), e);
        }
    }

    private static String decode(ByteBuffer buffer) {
        if (buffer == null) {
            return "";
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Map<String, String> headers(Record record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            if (header.value() != null) {
                headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        // A missing header resolves to "" so `headers.tenant == "acme"` is `false` rather than
        // raising a CEL "key not present in map" evaluation error. Presence can still be tested
        // explicitly with the `has(headers.tenant)` macro.
        return new DefaultingMap(headers);
    }

    /// A map that returns `""` for absent keys, so header lookups in CEL expressions are
    /// falsy instead of erroring when a header is missing.
    static final class DefaultingMap extends HashMap<String, String> {

        DefaultingMap(Map<String, String> headers) {
            super(headers);
        }

        @Override
        public String get(Object key) {
            return containsKey(key) ? super.get(key) : "";
        }
    }
}
