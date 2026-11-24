package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Renames one field of the record value. Moving it to another level flattens (`customer.name`
/// to `customerName`) or nests (`city` to `address.city`) it.
///
/// @param from the dotted path of the field (see [FieldPath])
/// @param to   the dotted path the field is exposed under
/// @param errorHeader see [TransformationConfig#errorHeader()]; `true` when omitted
public record RenameFieldTransformationConfig(
        String from,
        String to,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements TransformationConfig {

    public RenameFieldTransformationConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        if (from == null || from.isBlank() || to == null || to.isBlank()) {
            throw new IllegalArgumentException("renameField transformation requires 'from' and 'to'");
        }
        FieldPath.parse(from);
        FieldPath.parse(to);
        if (from.equals(to) || to.startsWith(from + ".") || from.startsWith(to + ".")) {
            throw new IllegalArgumentException("renameField cannot move '" + from + "' to '" + to + "': one contains the other");
        }
    }

    public RenameFieldTransformationConfig(String from, String to) {
        this(from, to, true);
    }
}
