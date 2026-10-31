package io.jonasg.kawa.http.users;

import io.jonasg.kawa.http.web.Router;

import java.time.Clock;

/// Registers the `/admin/users` routes, backed by the [AdminUserService], on a [Router].
public final class UserRoutes {

    private UserRoutes() {
    }

    public static void register(Router router, AdminUserService adminUserService) {
        var mapper = new AdminUserMapper(Clock.systemUTC());
        router.get("/admin/users", new GetAdminUsersHandler(adminUserService, mapper))
                .post("/admin/users", new PostAdminUserHandler(adminUserService, mapper))
                .get("/admin/users/{id}", new GetAdminUserHandler(adminUserService, mapper))
                .patch("/admin/users/{id}", new PatchAdminUserHandler(adminUserService, mapper))
                .delete("/admin/users/{id}", new DeleteAdminUserHandler(adminUserService));
    }
}
