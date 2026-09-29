package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /auth/clients/{name}`.
public final class PutAuthClientHandler implements Router.Handler {

    private final AuthClientService service;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PutAuthClientHandler(AuthClientService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        ClientConfigRequest body;
        try {
            body = mapper.readValue(request.body(), ClientConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid client body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var client = service.upsertClient(name, body, consistency);
            return Router.Response.ok(new ClientConfigView(name, client.mechanism()));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
