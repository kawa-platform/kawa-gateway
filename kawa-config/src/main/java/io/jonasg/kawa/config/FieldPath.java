package io.jonasg.kawa.config;

import java.util.List;

/// A dotted path to a field of a record value, such as `customer.name`. Used by the field
/// transformation steps; a field whose own name contains a dot cannot be addressed this way.
public final class FieldPath {

    private FieldPath() {
    }

    /// Splits `path` into its segments.
    ///
    /// @throws IllegalArgumentException if `path` is blank or has an empty segment (`a..b`, `.a`, `a.`)
    public static List<String> parse(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("a field path must not be blank");
        }
        List<String> segments = List.of(path.split("\\.", -1));
        if (segments.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("field path '" + path + "' has an empty segment");
        }
        return segments;
    }
}
