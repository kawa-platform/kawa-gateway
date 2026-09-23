package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
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

public class CelRecordPredicate implements RecordPredicate {

    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .addVar("key", SimpleType.STRING)
            .addVar("value", SimpleType.STRING)
            .addVar("headers", MapType.create(SimpleType.STRING, SimpleType.STRING))
            .addVar("timestamp", SimpleType.INT)
            .build();
    private static final CelRuntime RUNTIME = CelRuntimeFactory.plannerRuntimeBuilder().build();

    /// The compiled expression. Compilation is expensive, so it happens once here - which also
    /// rejects an invalid expression when the filter is created rather than on the first record.
    /// `CelRuntime.Program` is thread-safe and side-effect free, so the instance can be shared.
    private final CelRuntime.Program program;
    private final String expression;

    public CelRecordPredicate(CelFilterConfig config) {
        this.expression = config.expression();
        this.program = compile(expression);
    }

    @Override
    public boolean test(Record record) {
        Map<String, Object> bindings = new HashMap<>(4);
        bindings.put("key", decode(record.key()));
        bindings.put("value", decode(record.value()));
        bindings.put("headers", headers(record));
        bindings.put("timestamp", record.timestamp());
        try {
            return Boolean.TRUE.equals(program.eval(bindings));
        } catch (CelEvaluationException e) {
            throw new IllegalStateException(
                    "Failed to evaluate CEL filter '" + expression + "': " + e.getMessage(), e);
        }
    }

    private static CelRuntime.Program compile(String expression) {
        try {
            CelAbstractSyntaxTree ast = COMPILER.compile(expression).getAst();
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
