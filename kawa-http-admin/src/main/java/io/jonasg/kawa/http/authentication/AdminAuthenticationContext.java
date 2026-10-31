package io.jonasg.kawa.http.authentication;

import io.jonasg.kawa.http.web.Router;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.ProviderSettings;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.netty.handler.codec.http.HttpMethod;

/// The [AuthenticationProviderContext] the admin server hands to the selected provider: its
/// settings, kawa's admin users, the admin API's public URL, and public endpoints registered straight
/// into the [Router].
public final class AdminAuthenticationContext implements AuthenticationProviderContext {

    private final ProviderSettings settings;
    private final AdminUserDirectory adminUsers;
    private final String publicUrl;
    private final Router router;

    public AdminAuthenticationContext(ProviderSettings settings, AdminUserDirectory adminUsers, String publicUrl, Router router) {
        this.settings = settings;
        this.adminUsers = adminUsers;
        this.publicUrl = publicUrl;
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
    public String publicUrl() {
        return publicUrl;
    }

    @Override
    public void publicEndpoint(String method, String path, HttpEndpoint endpoint) {
        router.publicRoute(HttpMethod.valueOf(method), path, new HttpEndpointAdapter(endpoint));
    }
}
