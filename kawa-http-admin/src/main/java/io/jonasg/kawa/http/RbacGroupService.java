package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;

import java.util.Map;

/// Owns the RBAC group section of the gateway config snapshot, including rename
/// semantics and referential-integrity checks.
final class RbacGroupService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    RbacGroupService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, GroupConfig> listGroups() {
        return repository.getActiveConfigOrEmpty().rbac().groups();
    }

    GroupConfig upsertGroup(String name, GroupConfigRequest request, Consistency consistency) {
        GroupConfig value = new GroupConfig(request.clients(), request.roles());
        updater.update(consistency, config -> config.updateRbac(config.rbac().upsertGroup(name, value)));
        return value;
    }

    GroupConfig updateGroup(String name, GroupConfigPatch patch, Consistency consistency) {
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
        updater.update(consistency, config -> config.updateRbac(config.rbac().renameGroup(name, newName)));
        return new GroupConfig(current.clients(), current.roles());
    }

    void deleteGroup(String name, Consistency consistency) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        GroupConfig group = base.rbac().groups().get(name);
        if (group == null) {
            throw new NotFoundException("group '" + name + "' not found");
        }
        if (!group.clients().isEmpty()) {
            throw new ConflictException("group '" + name + "' still has clients " + group.clients());
        }
        updater.update(consistency, config -> config.updateRbac(config.rbac().removeGroup(name)));
    }
}
