package io.jonasg.kawa.http.auth.oauth;

import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.ProviderSettings;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// An in-memory [AuthenticationProviderContext]: settings from a map, admin users with plaintext
/// passwords, and the registered public endpoints collected by `METHOD path`.
final class FakeProviderContext implements AuthenticationProviderContext {

    private final Map<String, String> settings;
    private final @Nullable String bootstrapUsername;
    private final Map<String, String> users;
    final Map<String, HttpEndpoint> endpoints = new HashMap<>();

    FakeProviderContext(Map<String, String> settings, @Nullable String bootstrapUsername, Map<String, String> users) {
        this.settings = settings;
        this.bootstrapUsername = bootstrapUsername;
        this.users = users;
    }

    @Override
    public ProviderSettings settings() {
        return new ProviderSettings() {
            @Override
            public Optional<String> string(String key) {
                return Optional.ofNullable(settings.get(key));
            }

            @Override
            public String requireString(String key) {
                return string(key).orElseThrow(() -> new IllegalArgumentException("admin.auth.settings." + key + " is required"));
            }

            @Override
            public Optional<Duration> duration(String key) {
                return string(key).map(Duration::parse);
            }

            @Override
            public List<String> strings(String key) {
                return string(key).map(value -> Arrays.stream(value.split(",")).map(String::trim).toList()).orElse(List.of());
            }
        };
    }

    @Override
    public AdminUserDirectory adminUsers() {
        return new AdminUserDirectory() {
            @Override
            public boolean verify(String username, String password) {
                return password.equals(users.get(username));
            }

            @Override
            public boolean exists(String username) {
                return users.containsKey(username);
            }

            @Override
            public Optional<String> bootstrapUsername() {
                return Optional.ofNullable(bootstrapUsername);
            }
        };
    }

    @Override
    public void publicEndpoint(String method, String path, HttpEndpoint endpoint) {
        endpoints.put(method + " " + path, endpoint);
    }
}
