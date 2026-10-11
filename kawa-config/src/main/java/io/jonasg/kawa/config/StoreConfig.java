package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;

/// A TLS key or trust store: where it is, its format and how to open it. Passwords may reference
/// environment variables (`${VAR}` or `${VAR:-default}`).
///
/// @param location    path of the store file
/// @param type        the file's format (defaults to [StoreType#PKCS12])
/// @param password    the store password; required for PKCS12 and JKS, not allowed for PEM (an
///                    encrypted PEM key takes `keyPassword`, as in Kafka)
/// @param keyPassword the private key's password in a keystore, when it differs from the store
///                    password or the PEM key is encrypted; unused in a truststore
public record StoreConfig(
        String location,
        StoreType type,
        @Nullable String password,
        @Nullable String keyPassword
) {

    public StoreConfig {
        if (location == null || location.isBlank()) {
            throw new IllegalArgumentException("TLS store location must not be null or blank");
        }
        if (!Files.isRegularFile(Path.of(location))) {
            throw new IllegalArgumentException("TLS store '" + location + "' does not exist or is not a file");
        }
        if (type == null) {
            type = StoreType.PKCS12;
        }
        if (password != null) {
            password = ClientConfig.resolveEnvVars(password, System::getenv);
        }
        if (type == StoreType.PEM && password != null) {
            throw new IllegalArgumentException("TLS store '" + location + "': a PEM file has no store password;"
                    + " use keyPassword for an encrypted PEM key");
        }
        if (type != StoreType.PEM && (password == null || password.isBlank())) {
            throw new IllegalArgumentException("TLS store '" + location + "': a password is required for " + type);
        }
        if (keyPassword != null) {
            keyPassword = ClientConfig.resolveEnvVars(keyPassword, System::getenv);
        }
    }
}
