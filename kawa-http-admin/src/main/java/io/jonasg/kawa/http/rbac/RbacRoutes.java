package io.jonasg.kawa.http.rbac;

import io.jonasg.kawa.http.configchange.DeleteManagedByHandler;
import io.jonasg.kawa.http.web.Router;

/// Registers the `/rbac/roles` and `/rbac/groups` routes, backed by the [RbacRoleService] and
/// [RbacGroupService], on a [Router].
public final class RbacRoutes {

    private RbacRoutes() {
    }

    public static void register(Router router, RbacRoleService roleService, RbacGroupService groupService) {
        router.get("/rbac/roles", new GetRbacRolesHandler(roleService))
                .put("/rbac/roles/{name}", new PutRbacRoleHandler(roleService))
                .delete("/rbac/roles/{name}", new DeleteRbacRoleHandler(roleService))
                .delete("/rbac/roles/{name}/managed-by", new DeleteManagedByHandler(roleService::releaseRole))
                .get("/rbac/groups", new GetRbacGroupsHandler(groupService))
                .put("/rbac/groups/{name}", new PutRbacGroupHandler(groupService))
                .delete("/rbac/groups/{name}", new DeleteRbacGroupHandler(groupService))
                .patch("/rbac/groups/{name}", new PatchRbacGroupHandler(groupService))
                .delete("/rbac/groups/{name}/managed-by", new DeleteManagedByHandler(groupService::releaseGroup));
    }
}
