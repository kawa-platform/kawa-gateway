package io.jonasg.kawa.config;

/// Renames one field of the record value. Moving it to another level flattens (`customer.name`
/// to `customerName`) or nests (`city` to `address.city`) it.
///
/// @param from the dotted path of the field (see [FieldPath])
/// @param to   the dotted path the field is exposed under
public record RenameFieldTransformationConfig(String from, String to) implements TransformationConfig {

    public RenameFieldTransformationConfig {
        if (from == null || from.isBlank() || to == null || to.isBlank()) {
            throw new IllegalArgumentException("renameField transformation requires 'from' and 'to'");
        }
        FieldPath.parse(from);
        FieldPath.parse(to);
        if (from.equals(to) || to.startsWith(from + ".") || from.startsWith(to + ".")) {
            throw new IllegalArgumentException("renameField cannot move '" + from + "' to '" + to + "': one contains the other");
        }
    }
}
