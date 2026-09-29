package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PATCH /auth/clients/{name}`.
public final class PatchAuthClientHandler implements Router.Handler {

    private final AuthClientService service;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PatchAuthClientHandler(AuthClientService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        ClientConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), ClientConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid client body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var client = service.updateClient(name, patch, consistency);
            return Router.Response.ok(new ClientConfigView(name, client.mechanism()));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
