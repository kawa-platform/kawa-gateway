package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /admin/users/{name}`: creates the admin user or replaces their password.
public final class PutAdminUserHandler implements Router.Handler {

    private final AdminUserService service;
    private final AdminUserMapper mapper;
    private final JsonMapper json = JsonMapper.builder().build();

    PutAdminUserHandler(AdminUserService service, AdminUserMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        AdminUserRequest body;
        try {
            body = json.readValue(request.body(), AdminUserRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid admin user body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            service.upsertAdminUser(name, mapper.toAdminUserConfig(name, body), consistency);
            return Router.Response.ok(mapper.toAdminUserView(name));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
