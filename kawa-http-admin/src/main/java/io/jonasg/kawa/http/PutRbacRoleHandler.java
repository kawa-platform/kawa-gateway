package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /rbac/roles/{name}`.
public final class PutRbacRoleHandler implements Router.Handler {

    private final RbacRoleService service;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PutRbacRoleHandler(RbacRoleService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        RoleConfigRequest body;
        try {
            body = mapper.readValue(request.body(), RoleConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid role body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var value = service.upsertRole(name, body, consistency);
            return Router.Response.ok(new RolePutView(value.acls()));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
