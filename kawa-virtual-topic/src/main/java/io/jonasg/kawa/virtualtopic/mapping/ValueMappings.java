package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.CelTransformationConfig;
import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DefaultFieldTransformationConfig;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import org.apache.kafka.common.record.internal.Record;

import java.util.ArrayList;
import java.util.List;

/// Compiles a virtual topic's transformation steps into one [ValueMapping], applied in order to
/// the decoded record value on fetch.
///
/// | Step           | Effect                                                                    |
/// |----------------|---------------------------------------------------------------------------|
/// | `dropFields`   | removes fields ([DropFieldsMapping])                                      |
/// | `renameField`  | moves a field, also flattening or nesting it ([RenameFieldMapping])       |
/// | `defaultField` | sets an absent or `null` field ([DefaultFieldMapping])                    |
/// | `cel`          | replaces the value with a CEL expression's result ([CelMapping])          |
/// | `convert`      | none here: it picks the format the result is encoded in                   |
///
/// Fields are addressed by dotted paths; a list on a path applies the step to every element.
/// Picking one element of a list or building a new list is done with `cel` (`value.lines.map(l,
/// l.sku)`).
///
/// Compiling validates every step - a CEL expression that does not compile fails here, when the
/// configuration loads, not on the first record.
///
/// Mappings are one-way: dropping or overwriting a field cannot be undone, so a mapping is never
/// claimed to be invertible and there is no produce direction.
///
/// Every step terminates in time linear in the value's size: paths are finite, value trees are
/// acyclic (decoders build them, CEL values cannot reference themselves), and CEL has no
/// recursion - its macros iterate over the input only. Nesting depth is already bounded by the
/// parsers (Jackson's stream read constraints, Woodstox's maximum element depth).
public final class ValueMappings {

    private ValueMappings() {
    }

    /// @throws IllegalArgumentException naming the step if one of `steps` is invalid
    public static ValueMapping compile(List<TransformationConfig> steps) {
        List<ValueMapping> mappings = new ArrayList<>();
        for (int i = 0; i < steps.size(); i++) {
            TransformationConfig step = steps.get(i);
            String label = "transformation #" + (i + 1) + " (" + type(step) + ")";
            ValueMapping mapping;
            try {
                mapping = switch (step) {
                    case DropFieldsTransformationConfig config -> new DropFieldsMapping(config);
                    case RenameFieldTransformationConfig config -> new RenameFieldMapping(config);
                    case DefaultFieldTransformationConfig config -> new DefaultFieldMapping(config);
                    case CelTransformationConfig config -> new CelMapping(config);
                    case ConvertTransformationConfig _ -> null;
                };
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(label + ": " + e.getMessage(), e);
            }
            if (mapping != null) {
                mappings.add(labelled(label, mapping));
            }
        }
        return switch (mappings.size()) {
            case 0 -> ValueMapping.IDENTITY;
            case 1 -> mappings.getFirst();
            default -> new Pipeline(List.copyOf(mappings));
        };
    }

    private static ValueMapping labelled(String label, ValueMapping mapping) {
        return (value, record) -> {
            try {
                return mapping.apply(value, record);
            } catch (PayloadMappingException e) {
                throw new PayloadMappingException(label + ": " + e.getMessage(), e);
            }
        };
    }

    private static String type(TransformationConfig step) {
        return switch (step) {
            case DropFieldsTransformationConfig _ -> "dropFields";
            case RenameFieldTransformationConfig _ -> "renameField";
            case DefaultFieldTransformationConfig _ -> "defaultField";
            case CelTransformationConfig _ -> "cel";
            case ConvertTransformationConfig _ -> "convert";
        };
    }

    private record Pipeline(List<ValueMapping> steps) implements ValueMapping {

        @Override
        public Object apply(Object value, Record record) {
            Object result = value;
            for (ValueMapping step : steps) {
                result = step.apply(result, record);
            }
            return result;
        }
    }
}
