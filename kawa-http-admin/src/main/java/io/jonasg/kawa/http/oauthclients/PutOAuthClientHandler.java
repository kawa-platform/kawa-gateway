package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.web.Router;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /admin/oauth-clients/{id}`: adds or replaces an OAuth client. The id comes from the path. An invalid
/// body is a `400`; the reserved id `kawa-studio` is a `409`.
final class PutOAuthClientHandler implements Router.Handler {

    private final OAuthClientService service;
    private final OAuthClientConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    PutOAuthClientHandler(OAuthClientService service, OAuthClientConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        OAuthClientRequest clientReq;
        try {
            clientReq = jsonMapper.readValue(request.body(), OAuthClientRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid OAuth client body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var client = mapper.toOAuthClientConfig(request.pathParams().get("id"), clientReq);
            return Router.Response.ok(mapper.toOAuthClientConfigView(service.upsertOAuthClient(client, consistency)));
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
