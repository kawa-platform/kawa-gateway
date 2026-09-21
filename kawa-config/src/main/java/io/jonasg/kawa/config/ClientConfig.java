package io.jonasg.kawa.config;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ClientConfig(
        String mechanism,
        HashedPassword password
) {

    private static final Pattern ENV_VAR_PATTERN =
            Pattern.compile("\\$\\{([^}:]+)(?::-(.+?))?\\}");

    public ClientConfig {
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
    }

    /// Creates a client configuration by hashing a plaintext password before it enters the
    /// configuration model.
    public static ClientConfig fromPlaintext(String mechanism, String plaintext) {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("mechanism must not be null or blank");
        }
        return new ClientConfig(mechanism,
                HashedPassword.fromPlaintext(Mechanism.fromWireName(mechanism), plaintext));
    }

    /// Resolves `${VAR}` and `${VAR:-default}` placeholders in a value against the given lookup.
    ///
    /// Used by `BrokerAuthConfig` for broker credentials; client passwords are hashed before
    /// construction and never pass through environment resolution.
    public static String resolveEnvVars(String value, Function<String, String> envLookup) {
        Matcher matcher = ENV_VAR_PATTERN.matcher(value);
        if (!matcher.find()) {
            return value;
        }
        matcher.reset();
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String defaultValue = matcher.group(2);
            String resolved = envLookup.apply(varName);
            if (resolved == null) {
                if (defaultValue != null) {
                    resolved = defaultValue;
                } else {
                    throw new IllegalArgumentException(
                            "Environment variable '" + varName
                            + "' is not set and no default is configured");
                }
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(resolved));
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
