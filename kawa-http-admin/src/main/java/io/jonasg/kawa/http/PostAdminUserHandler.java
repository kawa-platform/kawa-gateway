package io.jonasg.kawa.http;

/// Serves `POST /admin/users`: creates an admin user under a new id and answers `201` with the
/// stored user and its `Location`. An email in use, also by the bootstrap user, is a `409`.
public final class PostAdminUserHandler implements Router.Handler {

    private final AdminUserService service;
    private final AdminUserMapper mapper;

    PostAdminUserHandler(AdminUserService service, AdminUserMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        try {
            var body = AdminUserRequests.read(request.body(), CreateAdminUserRequest.class);
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var user = mapper.toNewAdminUser(body);
            String id = service.createAdminUser(user, consistency);
            return Router.Response.created(mapper.toAdminUserView(id, user)).withHeader("Location", "/admin/users/" + id);
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
