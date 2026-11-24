package io.jonasg.kawa.virtualtopic.mapping;

import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.virtualtopic.filter.RecordCel;
import org.apache.kafka.common.record.internal.Record;

/// Replaces the value with the result of a CEL expression, compiled once in the
/// [RecordCel.Environment#DECODED_MAPPING] environment. The result is converted into the value
/// tree, so maps need string keys, and CEL values without a counterpart (`uint`, `timestamp`,
/// `duration`) must be converted in the expression first.
final class CelMapping implements ValueMapping {

    private final CelRuntime.Program program;

    /// @throws IllegalArgumentException if the expression does not compile
    CelMapping(CelTransformationConfig config) {
        this.program = RecordCel.compile(RecordCel.Environment.DECODED_MAPPING, config.expression());
    }

    @Override
    public Object apply(Object value, Record record) {
        try {
            return ValueTree.of(program.eval(RecordCel.bindings(record, value)));
        } catch (CelEvaluationException e) {
            // the evaluator's message can quote the value it failed on; its error code never does
            throw new PayloadMappingException("evaluation failed: " + e.getErrorCode(), e);
        }
    }
}
