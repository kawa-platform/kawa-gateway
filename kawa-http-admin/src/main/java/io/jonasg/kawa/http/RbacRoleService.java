package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;

import java.util.Map;

/// Owns the RBAC role section of the gateway config snapshot, including cascading
/// removal of roles from groups. Every write honours [Ownership]: a managed role, and a managed
/// group a role removal would change, only change through their owner.
final class RbacRoleService {

    private static final String KIND = "role";

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    RbacRoleService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, RoleConfig> listRoles() {
        return repository.getActiveConfigOrEmpty().rbac().roles();
    }

    RoleConfig upsertRole(KawaPrincipal caller, String name, RoleConfigRequest request, Consistency consistency) {
        RoleConfig value = new RoleConfig(request.acls(), Ownership.stamp(caller));
        updater.update(consistency, config -> {
            requireRoleOwner(config, name, caller);
            return config.updateRbac(config.rbac().upsertRole(name, value));
        });
        return value;
    }

    void deleteRole(KawaPrincipal caller, String name, Consistency consistency) {
        if (!repository.getActiveConfigOrEmpty().rbac().roles().containsKey(name)) {
            throw new NotFoundException("role '" + name + "' not found");
        }
        updater.update(consistency, config -> {
            requireRoleOwner(config, name, caller);
            config.rbac().groups().forEach((groupName, group) -> {
                if (group.roles().contains(name)) {
                    Ownership.requireOwner("group", groupName, group.managedBy(), caller);
                }
            });
            return config.updateRbac(config.rbac().removeRole(name));
        });
    }

    /// Clears the role's owner. Only the owner may release it; an unmanaged role is left as is.
    void releaseRole(KawaPrincipal caller, String name, Consistency consistency) {
        RoleConfig current = repository.getActiveConfigOrEmpty().rbac().roles().get(name);
        if (current == null) {
            throw new NotFoundException("role '" + name + "' not found");
        }
        if (current.managedBy() == null) {
            return;
        }
        updater.update(consistency, config -> {
            RoleConfig role = config.rbac().roles().get(name);
            if (role == null) {
                throw new NotFoundException("role '" + name + "' not found");
            }
            Ownership.requireOwner(KIND, name, role.managedBy(), caller);
            return config.updateRbac(config.rbac().upsertRole(name, role.withManagedBy(null)));
        });
    }

    private static void requireRoleOwner(GatewayConfig config, String name, KawaPrincipal caller) {
        RoleConfig existing = config.rbac().roles().get(name);
        if (existing != null) {
            Ownership.requireOwner(KIND, name, existing.managedBy(), caller);
        }
    }
}
