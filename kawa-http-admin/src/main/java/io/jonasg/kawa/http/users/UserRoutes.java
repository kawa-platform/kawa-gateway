package io.jonasg.kawa.http.users;

import io.jonasg.kawa.http.web.Router;
import io.netty.handler.codec.http.HttpMethod;

import java.time.Clock;

/// Registers the `/admin/users` routes, backed by the [AdminUserService], and the public `/admin/setup`
/// routes, backed by the [AdminSetup], on a [Router].
public final class UserRoutes {

    private UserRoutes() {
    }

    public static void register(Router router, AdminUserService adminUserService, AdminSetup setup) {
        var mapper = new AdminUserMapper(Clock.systemUTC());
        router.get("/admin/users", new GetAdminUsersHandler(adminUserService, mapper))
                .post("/admin/users", new PostAdminUserHandler(adminUserService, mapper))
                .get("/admin/users/{id}", new GetAdminUserHandler(adminUserService, mapper))
                .patch("/admin/users/{id}", new PatchAdminUserHandler(adminUserService, mapper))
                .delete("/admin/users/{id}", new DeleteAdminUserHandler(adminUserService))
                .publicGet("/admin/setup", new GetSetupHandler(setup))
                .publicRoute(HttpMethod.POST, "/admin/setup", new PostSetupHandler(setup, mapper));
    }
}
