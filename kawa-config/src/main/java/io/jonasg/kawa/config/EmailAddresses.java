package io.jonasg.kawa.config;

import java.util.Locale;
import java.util.regex.Pattern;

/// The email address check for admin usernames. Deliberately loose: a local part and a domain
/// around a single `@`, without whitespace. The domain needs no dot, so `admin@kawa` is valid.
/// Admin usernames are case-insensitive, so they are compared and stored in lower case.
public final class EmailAddresses {

    private static final Pattern EMAIL = Pattern.compile("[^@\\s]+@[^@\\s]+");

    private EmailAddresses() {
    }

    /// Whether `value` looks like an email address.
    public static boolean isValid(String value) {
        return value != null && EMAIL.matcher(value).matches();
    }

    /// `value` trimmed and in lower case, the form admin emails are stored and compared in.
    ///
    /// @throws IllegalArgumentException when `value` is `null` or not an email address
    public static String normalize(String value) {
        if (value == null) {
            throw new IllegalArgumentException("email must not be null");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (!isValid(normalized)) {
            throw new IllegalArgumentException("'" + normalized + "' is not an email address");
        }
        return normalized;
    }
}
