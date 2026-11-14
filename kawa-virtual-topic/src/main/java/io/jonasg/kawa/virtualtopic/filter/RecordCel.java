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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/// The CEL environment record expressions (filters and `cel` transformations) are compiled and
/// evaluated in: the `key`, `value`, `headers` and `timestamp` bindings of a record.
///
/// A compiled `CelRuntime.Program` is thread-safe and side-effect free, so it is compiled once
/// per expression and shared.
public final class RecordCel {

    /// Heterogeneous numeric comparisons let `value.amount > 100` work whether the JSON number
    /// decoded to a `Long` or a `Double`.
    private static final CelOptions OPTIONS = CelOptions.current()
            .enableHeterogeneousNumericComparisons(true)
            .build();

    private static final CelRuntime RUNTIME = CelRuntimeFactory.plannerRuntimeBuilder()
            .setOptions(OPTIONS)
            .build();

    /// Which form `value` is bound in, and which macros an expression may use.
    public enum Environment {

        /// A filter over the raw value, bound as a string. Only the `has` macro.
        RAW_FILTER(SimpleType.STRING, CelStandardMacro.HAS),

        /// A filter over the decoded value tree (`dyn`: a map, list or scalar). Only the `has` macro.
        DECODED_FILTER(SimpleType.DYN, CelStandardMacro.HAS),

        /// A mapping over the decoded value tree, with every standard macro (`map`, `filter`,
        /// `exists`, ...) so it can map into and out of lists.
        DECODED_MAPPING(SimpleType.DYN, CelStandardMacro.STANDARD_MACROS.toArray(CelStandardMacro[]::new));

        private final CelCompiler compiler;

        Environment(CelType valueType, CelStandardMacro... macros) {
            this.compiler = CelCompilerFactory.standardCelCompilerBuilder()
                    .setOptions(OPTIONS)
                    .setStandardMacros(macros)
                    .addVar("key", SimpleType.STRING)
                    .addVar("value", valueType)
                    .addVar("headers", MapType.create(SimpleType.STRING, SimpleType.STRING))
                    .addVar("timestamp", SimpleType.INT)
                    .build();
        }
    }

    private RecordCel() {
    }

    /// @throws IllegalArgumentException if `expression` does not compile in `environment`
    public static CelRuntime.Program compile(Environment environment, String expression) {
        try {
            CelAbstractSyntaxTree ast = environment.compiler.compile(expression).getAst();
            return RUNTIME.createProgram(ast);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException("Invalid CEL expression '" + expression + "': " + e.getMessage(), e);
        }
    }

    /// The bindings of `record`, with `value` bound as given.
    public static Map<String, Object> bindings(Record record, Object value) {
        Map<String, Object> bindings = new HashMap<>(4);
        bindings.put("key", text(record.key()));
        bindings.put("value", value);
        bindings.put("headers", headers(record));
        bindings.put("timestamp", record.timestamp());
        return bindings;
    }

    /// A record key or value as UTF-8 text, `""` when absent.
    static String text(ByteBuffer buffer) {
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
