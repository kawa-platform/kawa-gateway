package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;

import java.util.Map;

/// Owns the admin users section of the gateway config snapshot.
final class AdminUserService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    AdminUserService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, AdminUserConfig> listAdminUsers() {
        return repository.getActiveConfigOrEmpty().adminUsers();
    }

    void upsertAdminUser(String name, AdminUserConfig user, Consistency consistency) {
        updater.update(consistency, config -> config.upsertAdminUser(name, user));
    }

    void deleteAdminUser(String name, Consistency consistency) {
        if (!listAdminUsers().containsKey(name)) {
            throw new NotFoundException("admin user '" + name + "' not found");
        }
        updater.update(consistency, config -> config.removeAdminUser(name));
    }
}
