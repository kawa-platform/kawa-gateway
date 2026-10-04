package io.jonasg.kawa.http;

/// Serves `GET /admin/users`.
public final class GetAdminUsersHandler implements Router.Handler {

    private final AdminUserService service;
    private final AdminUserMapper mapper;

    GetAdminUsersHandler(AdminUserService service, AdminUserMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(service.listAdminUsers().keySet().stream()
                .sorted()
                .map(mapper::toAdminUserView)
                .toList());
    }
}
