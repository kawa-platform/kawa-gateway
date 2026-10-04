package io.jonasg.kawa.http;

import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.ProviderSettings;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.netty.handler.codec.http.HttpMethod;

/// The [AuthenticationProviderContext] the admin server hands to the selected provider: its
/// settings, kawa's admin users, and public endpoints registered straight into the [Router].
final class AdminAuthenticationContext implements AuthenticationProviderContext {

    private final ProviderSettings settings;
    private final AdminUserDirectory adminUsers;
    private final Router router;

    AdminAuthenticationContext(ProviderSettings settings, AdminUserDirectory adminUsers, Router router) {
        this.settings = settings;
        this.adminUsers = adminUsers;
        this.router = router;
    }

    @Override
    public ProviderSettings settings() {
        return settings;
    }

    @Override
    public AdminUserDirectory adminUsers() {
        return adminUsers;
    }

    @Override
    public void publicEndpoint(String method, String path, HttpEndpoint endpoint) {
        router.publicRoute(HttpMethod.valueOf(method), path, new HttpEndpointAdapter(endpoint));
    }
}
