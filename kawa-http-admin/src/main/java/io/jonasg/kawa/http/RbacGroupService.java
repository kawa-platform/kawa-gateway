package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;

import java.util.Map;

/// Owns the RBAC group section of the gateway config snapshot, including rename
/// semantics and referential-integrity checks. Every write honours [Ownership]: a managed group
/// only changes through its owner.
final class RbacGroupService {

    private static final String KIND = "group";

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    RbacGroupService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, GroupConfig> listGroups() {
        return repository.getActiveConfigOrEmpty().rbac().groups();
    }

    GroupConfig upsertGroup(KawaPrincipal caller, String name, GroupConfigRequest request, Consistency consistency) {
        GroupConfig value = new GroupConfig(request.clients(), request.roles(), Ownership.stamp(caller));
        updater.update(consistency, config -> {
            GroupConfig existing = config.rbac().groups().get(name);
            if (existing != null) {
                Ownership.requireOwner(KIND, name, existing.managedBy(), caller);
            }
            return config.updateRbac(config.rbac().upsertGroup(name, value));
        });
        return value;
    }

    GroupConfig updateGroup(KawaPrincipal caller, String name, GroupConfigPatch patch, Consistency consistency) {
        String newName = patch.name();
        if (newName == null || newName.isBlank()) {
            throw new IllegalArgumentException("no new name to rename to");
        }
        if (newName.equals(name)) {
            throw new IllegalArgumentException("group is already named '" + name + "'");
        }
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        if (base.rbac().groups().containsKey(newName)) {
            throw new ConflictException("group '" + newName + "' already exists");
        }
        GroupConfig current = base.rbac().groups().get(name);
        if (current == null) {
            throw new NotFoundException("group '" + name + "' not found");
        }
        var stamp = Ownership.stamp(caller);
        updater.update(consistency, config -> {
            GroupConfig group = config.rbac().groups().get(name);
            if (group == null) {
                return config;
            }
            Ownership.requireOwner(KIND, name, group.managedBy(), caller);
            return config.updateRbac(config.rbac().removeGroup(name).upsertGroup(newName, group.withManagedBy(stamp)));
        });
        return current.withManagedBy(stamp);
    }

    void deleteGroup(KawaPrincipal caller, String name, Consistency consistency) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        GroupConfig group = base.rbac().groups().get(name);
        if (group == null) {
            throw new NotFoundException("group '" + name + "' not found");
        }
        if (!group.clients().isEmpty()) {
            throw new ConflictException("group '" + name + "' still has clients " + group.clients());
        }
        updater.update(consistency, config -> {
            GroupConfig existing = config.rbac().groups().get(name);
            if (existing != null) {
                Ownership.requireOwner(KIND, name, existing.managedBy(), caller);
            }
            return config.updateRbac(config.rbac().removeGroup(name));
        });
    }

    /// Clears the group's owner. Only the owner may release it; an unmanaged group is left as is.
    void releaseGroup(KawaPrincipal caller, String name, Consistency consistency) {
        GroupConfig current = repository.getActiveConfigOrEmpty().rbac().groups().get(name);
        if (current == null) {
            throw new NotFoundException("group '" + name + "' not found");
        }
        if (current.managedBy() == null) {
            return;
        }
        updater.update(consistency, config -> {
            GroupConfig group = config.rbac().groups().get(name);
            if (group == null) {
                throw new NotFoundException("group '" + name + "' not found");
            }
            Ownership.requireOwner(KIND, name, group.managedBy(), caller);
            return config.updateRbac(config.rbac().upsertGroup(name, group.withManagedBy(null)));
        });
    }
}
