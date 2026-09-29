package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /rbac/groups/{name}`.
public final class PutRbacGroupHandler implements Router.Handler {

    private final RbacGroupService service;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PutRbacGroupHandler(RbacGroupService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        GroupConfigRequest body;
        try {
            body = mapper.readValue(request.body(), GroupConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid group body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var value = service.upsertGroup(name, body, consistency);
            return Router.Response.ok(new GroupPutView(value.clients(), value.roles()));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
