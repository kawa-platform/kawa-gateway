package io.jonasg.kawa.config;

/// HTTP basic authentication to a schema registry.
///
/// @param username the registry user
/// @param password its password; may reference environment variables (`${VAR}` or `${VAR:-default}`)
public record SchemaRegistryBasicAuthConfig(String username, String password) {

    public SchemaRegistryBasicAuthConfig {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("schemaRegistry.basicAuth.username is required");
        }
        if (password == null) {
            throw new IllegalArgumentException("schemaRegistry.basicAuth.password is required");
        }
        password = ClientConfig.resolveEnvVars(password, System::getenv);
    }

    /// Never prints the password.
    @Override
    public String toString() {
        return "SchemaRegistryBasicAuthConfig[username=" + username + ", password=***]";
    }
}
