package io.jonasg.kawa.http.rbac;

import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.http.web.Router;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/// Serves `GET /rbac/roles`.
final class GetRbacRolesHandler implements Router.Handler {

    private final RbacRoleService service;

    GetRbacRolesHandler(RbacRoleService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        Map<String, RoleConfig> roles = service.listRoles();
        List<RoleConfigView> views = roles.entrySet().stream()
                .map(entry -> new RoleConfigView(entry.getKey(), entry.getValue().acls(), entry.getValue().managedBy()))
                .sorted(Comparator.comparing(RoleConfigView::name))
                .toList();
        return Router.Response.ok(views);
    }
}
