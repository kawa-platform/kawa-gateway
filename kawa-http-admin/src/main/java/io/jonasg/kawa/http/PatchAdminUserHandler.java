package io.jonasg.kawa.http;

/// Serves `PATCH /admin/users/{id}`: changes the given fields of an admin user and keeps the rest, so
/// an operator can disable or rename a user without knowing their password. An email in use by
/// another admin user, or disabling your own admin user or the last enabled one, is a `409`.
public final class PatchAdminUserHandler implements Router.Handler {

    private final AdminUserService service;
    private final AdminUserMapper mapper;

    PatchAdminUserHandler(AdminUserService service, AdminUserMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String id = request.pathParams().get("id");
        try {
            var req = AdminUserRequests.read(request.body(), UpdateAdminUserRequest.class);
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var user = service.updateAdminUser(id, req, request.principalName(), consistency);
            return Router.Response.ok(mapper.toAdminUserView(id, user));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
