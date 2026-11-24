package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

/// Removes fields from the record value.
///
/// @param fields the dotted paths of the fields to remove (see [FieldPath]); a field the value
///               does not have is ignored
/// @param errorHeader see [TransformationConfig#errorHeader()]; `true` when omitted
public record DropFieldsTransformationConfig(
        List<String> fields,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements TransformationConfig {

    public DropFieldsTransformationConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("dropFields transformation requires at least one field");
        }
        fields = List.copyOf(fields);
        fields.forEach(FieldPath::parse);
    }

    public DropFieldsTransformationConfig(List<String> fields) {
        this(fields, true);
    }
}
