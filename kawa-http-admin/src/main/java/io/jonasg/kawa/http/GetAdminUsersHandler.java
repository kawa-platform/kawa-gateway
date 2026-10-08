package io.jonasg.kawa.http;

import java.util.Comparator;

/// Serves `GET /admin/users`: every stored admin user, sorted by email, including the bootstrap user
/// the gateway seeded on first start.
public final class GetAdminUsersHandler implements Router.Handler {

    private final AdminUserService service;
    private final AdminUserMapper mapper;

    GetAdminUsersHandler(AdminUserService service, AdminUserMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(service.listAdminUsers().entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getValue().email()))
                .map(entry -> mapper.toAdminUserView(entry.getKey(), entry.getValue()))
                .toList());
    }
}
