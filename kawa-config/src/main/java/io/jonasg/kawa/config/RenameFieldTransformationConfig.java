package io.jonasg.kawa.config;

/// Renames one field of the record value.
///
/// @param from the current field name
/// @param to   the name the field is exposed under
public record RenameFieldTransformationConfig(String from, String to) implements TransformationConfig {

    public RenameFieldTransformationConfig {
        if (from == null || from.isBlank() || to == null || to.isBlank()) {
            throw new IllegalArgumentException("renameField transformation requires 'from' and 'to'");
        }
    }
}
