package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.web.Router;

/// Serves `GET /admin/oauth-clients/{id}`: one stored OAuth client. An unknown id is a `404`.
final class GetOAuthClientHandler implements Router.Handler {

    private final OAuthClientService service;
    private final OAuthClientConfigMapper mapper;

    GetOAuthClientHandler(OAuthClientService service, OAuthClientConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        try {
            return Router.Response.ok(mapper.toOAuthClientConfigView(service.getOAuthClient(request.pathParams().get("id"))));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        }
    }
}
