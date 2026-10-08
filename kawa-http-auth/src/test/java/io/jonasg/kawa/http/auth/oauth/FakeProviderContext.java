package io.jonasg.kawa.http.auth.oauth;

import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.ProviderSettings;
import io.jonasg.kawa.plugin.http.HttpEndpoint;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// An in-memory [AuthenticationProviderContext]: settings from a map, admin users with plaintext
/// passwords (each user's subject is their username), and the registered public endpoints collected by `METHOD path`.
final class FakeProviderContext implements AuthenticationProviderContext {

    private final Map<String, String> settings;
    private final Map<String, String> users;
    final Map<String, HttpEndpoint> endpoints = new HashMap<>();

    FakeProviderContext(Map<String, String> settings, Map<String, String> users) {
        this.settings = settings;
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
            public Optional<String> authenticate(String username, String password) {
                return password.equals(users.get(username)) ? Optional.of(username) : Optional.empty();
            }

            @Override
            public Optional<String> username(String subject) {
                return users.containsKey(subject) ? Optional.of(subject) : Optional.empty();
            }

            @Override
            public boolean hasAdminUsers() {
                return !users.isEmpty();
            }
        };
    }

    @Override
    public void publicEndpoint(String method, String path, HttpEndpoint endpoint) {
        endpoints.put(method + " " + path, endpoint);
    }
}
