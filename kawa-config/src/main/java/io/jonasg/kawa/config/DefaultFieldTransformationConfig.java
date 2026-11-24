package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Sets a field of the record value when it is absent or `null`, creating missing parent objects.
///
/// @param field the dotted path of the field (see [FieldPath])
/// @param value the default: any JSON literal - string, number, boolean, `null`, object or list
/// @param errorHeader see [TransformationConfig#errorHeader()]; `true` when omitted
public record DefaultFieldTransformationConfig(
        String field,
        Object value,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements TransformationConfig {

    public DefaultFieldTransformationConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        FieldPath.parse(field);
    }

    public DefaultFieldTransformationConfig(String field, Object value) {
        this(field, value, true);
    }
}
