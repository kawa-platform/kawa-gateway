package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import io.jonasg.kawa.config.CelFilterConfig;
import org.apache.kafka.common.record.internal.Record;

/// Filters records with a CEL expression over the [RecordCel] bindings.
public class CelRecordPredicate implements RecordPredicate {

    /// The compiled expression. Compilation is expensive, so it happens once here - which also
    /// rejects an invalid expression when the filter is created rather than on the first record.
    private final CelRuntime.Program program;
    private final String expression;
    private final PayloadDecoder valueDecoder;

    /// @param valueDecoder decodes the record value before evaluation, or `null` to bind the
    ///                     raw value as a string
    public CelRecordPredicate(CelFilterConfig config, PayloadDecoder valueDecoder) {
        this.expression = config.expression();
        this.valueDecoder = valueDecoder;
        this.program = compile(config.expression(), valueDecoder == null
                ? RecordCel.Environment.RAW_FILTER
                : RecordCel.Environment.DECODED_FILTER);
    }

    @Override
    public boolean test(Record record) {
        Object value = valueDecoder == null ? RecordCel.text(record.value()) : valueDecoder.decode(record.value());
        try {
            return Boolean.TRUE.equals(program.eval(RecordCel.bindings(record, value)));
        } catch (CelEvaluationException e) {
            throw new IllegalStateException(
                    "Failed to evaluate CEL filter '" + expression + "': " + e.getMessage(), e);
        }
    }

    private static CelRuntime.Program compile(String expression, RecordCel.Environment environment) {
        try {
            return RecordCel.compile(environment, expression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid CEL filter expression '" + expression + "': " + e.getCause().getMessage(), e.getCause());
        }
    }
}
