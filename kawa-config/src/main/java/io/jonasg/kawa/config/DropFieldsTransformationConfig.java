package io.jonasg.kawa.config;

import java.util.List;

/// Removes fields from the record value.
///
/// @param fields the names of the fields to remove; a field the value does not have is ignored
public record DropFieldsTransformationConfig(List<String> fields) implements TransformationConfig {

    public DropFieldsTransformationConfig {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("dropFields transformation requires at least one field");
        }
        fields = List.copyOf(fields);
    }
}
