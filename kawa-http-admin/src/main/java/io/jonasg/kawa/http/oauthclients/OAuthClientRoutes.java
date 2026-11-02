package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.http.web.Router;

/// Registers the `/admin/oauth-clients` routes, backed by the [OAuthClientService], on a [Router].
public final class OAuthClientRoutes {

    private OAuthClientRoutes() {
    }

    public static void register(Router router, OAuthClientService oauthClientService) {
        var mapper = new OAuthClientConfigMapper();
        router.get("/admin/oauth-clients", new GetOAuthClientsHandler(oauthClientService, mapper))
                .get("/admin/oauth-clients/{id}", new GetOAuthClientHandler(oauthClientService, mapper))
                .put("/admin/oauth-clients/{id}", new PutOAuthClientHandler(oauthClientService, mapper))
                .delete("/admin/oauth-clients/{id}", new DeleteOAuthClientHandler(oauthClientService));
    }
}
