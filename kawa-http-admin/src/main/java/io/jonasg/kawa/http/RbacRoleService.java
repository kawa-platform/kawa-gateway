package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.RoleConfig;

import java.util.Map;

/// Owns the RBAC role section of the gateway config snapshot, including cascading
/// removal of roles from groups.
final class RbacRoleService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    RbacRoleService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, RoleConfig> listRoles() {
        return repository.getActiveConfigOrEmpty().rbac().roles();
    }

    RoleConfig upsertRole(String name, RoleConfigRequest request, Consistency consistency) {
        RoleConfig value = new RoleConfig(request.acls());
        updater.update(consistency, config -> config.updateRbac(config.rbac().upsertRole(name, value)));
        return value;
    }

    void deleteRole(String name, Consistency consistency) {
        if (!repository.getActiveConfigOrEmpty().rbac().roles().containsKey(name)) {
            throw new NotFoundException("role '" + name + "' not found");
        }
        updater.update(consistency, config -> config.updateRbac(config.rbac().removeRole(name)));
    }
}
