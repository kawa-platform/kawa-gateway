package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.RoleConfig;

import java.util.Comparator;
import java.util.Map;

final class RbacRolesCRUDHandler extends BaseCRUDHandler<RoleConfig, RoleConfigRequest> {

    RbacRolesCRUDHandler(GatewayConfigRepository repository) {
        super(repository, RoleConfigRequest.class, "role");
    }

    @Override
    protected Map<String, RoleConfig> entries(GatewayConfig config) {
        return config.rbac().roles();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new RoleConfigView(entry.getKey(), entry.getValue().acls()))
                .sorted(Comparator.comparing(RoleConfigView::name))
                .toList();
    }

    @Override
    protected RoleConfig toConfig(String name, RoleConfigRequest body) {
        return new RoleConfig(body.acls());
    }

    @Override
    protected Object putView(String name, RoleConfig value) {
        return new RoleValueView(value.acls());
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, RoleConfig value) {
        return config.updateRbac(config.rbac().upsertRole(name, value));
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateRbac(config.rbac().removeRole(name));
    }
}
