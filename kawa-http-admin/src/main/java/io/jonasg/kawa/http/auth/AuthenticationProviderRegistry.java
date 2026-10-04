package io.jonasg.kawa.http.auth;

import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;

import java.util.Map;
import java.util.TreeMap;

/// The admin API authentication providers registered by plugins (kawa's built-in `none` and `oauth`
/// included), keyed by [AuthenticationProviderFactory#name]. `admin.auth.type` selects one of them.
public final class AuthenticationProviderRegistry {

    private final Map<String, AuthenticationProviderFactory> factories = new TreeMap<>();

    /// Registers `factory`; a name that is already taken is rejected.
    public synchronized void register(AuthenticationProviderFactory factory) {
        var existing = factories.putIfAbsent(factory.name(), factory);
        if (existing != null) {
            throw new IllegalStateException("authentication provider '" + factory.name() + "' is registered by both "
                    + existing.getClass().getName() + " and " + factory.getClass().getName());
        }
    }

    /// The factory registered as `type`; fails naming the available providers when there is none.
    public synchronized AuthenticationProviderFactory require(String type) {
        var factory = factories.get(type);
        if (factory == null) {
            throw new IllegalArgumentException("no authentication provider '" + type + "' for admin.auth.type (available: "
                    + String.join(", ", factories.keySet()) + ")");
        }
        return factory;
    }
}
