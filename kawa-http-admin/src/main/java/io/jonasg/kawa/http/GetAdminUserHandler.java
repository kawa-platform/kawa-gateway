package io.jonasg.kawa.http;

/// Serves `GET /admin/users/{id}`.
public final class GetAdminUserHandler implements Router.Handler {

    private final AdminUserService service;
    private final AdminUserMapper mapper;

    GetAdminUserHandler(AdminUserService service, AdminUserMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String id = request.pathParams().get("id");
        try {
            return Router.Response.ok(mapper.toAdminUserView(id, service.getAdminUser(id)));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        }
    }
}
