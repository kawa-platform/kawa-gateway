package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.http.web.Router;

import java.util.Comparator;

/// Serves `GET /admin/oauth-clients`: every stored OAuth client, sorted by id.
final class GetOAuthClientsHandler implements Router.Handler {

    private final OAuthClientService service;
    private final OAuthClientConfigMapper mapper;

    GetOAuthClientsHandler(OAuthClientService service, OAuthClientConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(service.listOAuthClients().stream()
                .map(mapper::toOAuthClientConfigView)
                .sorted(Comparator.comparing(OAuthClientConfigView::id))
                .toList());
    }
}
