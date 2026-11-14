package io.jonasg.kawa.config;

/// Sets a field of the record value when it is absent or `null`, creating missing parent objects.
///
/// @param field the dotted path of the field (see [FieldPath])
/// @param value the default: any JSON literal - string, number, boolean, `null`, object or list
public record DefaultFieldTransformationConfig(String field, Object value) implements TransformationConfig {

    public DefaultFieldTransformationConfig {
        FieldPath.parse(field);
    }
}
