package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import io.jonasg.kawa.plugin.auth.ProviderSettings;
import io.jonasg.kawa.plugin.http.HttpEndpoint;

import java.util.function.Supplier;

/// Runs calls into an isolated plugin with the plugin's classloader as the thread context classloader.
///
/// kawa calls plugin code from its own threads, whose context classloader is kawa's. Libraries a plugin
/// bundles often look classes and resources up through the context classloader (`ServiceLoader`,
/// Jackson, JAXB, …), so without this they would search kawa's classpath instead of the plugin's.
final class IsolatedPluginCalls {

    private IsolatedPluginCalls() {
    }

    static <T> T withContextClassLoader(ClassLoader loader, Supplier<T> call) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return call.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /// `factory`, with every call into it and into what it creates run under `loader`.
    static AuthenticationProviderFactory isolate(AuthenticationProviderFactory factory, ClassLoader loader) {
        return new AuthenticationProviderFactory() {
            @Override
            public String name() {
                return factory.name();
            }

            @Override
            public AuthenticationProvider create(AuthenticationProviderContext context) {
                AuthenticationProvider provider = withContextClassLoader(loader,
                        () -> factory.create(isolate(context, loader)));
                return request -> withContextClassLoader(loader, () -> provider.authenticate(request));
            }

            @Override
            public String toString() {
                return factory.getClass().getName();
            }
        };
    }

    private static AuthenticationProviderContext isolate(AuthenticationProviderContext context, ClassLoader loader) {
        return new AuthenticationProviderContext() {
            @Override
            public ProviderSettings settings() {
                return context.settings();
            }

            @Override
            public AdminUserDirectory adminUsers() {
                return context.adminUsers();
            }

            @Override
            public void publicEndpoint(String method, String path, HttpEndpoint endpoint) {
                context.publicEndpoint(method, path, request -> withContextClassLoader(loader, () -> endpoint.handle(request)));
            }
        };
    }
}
