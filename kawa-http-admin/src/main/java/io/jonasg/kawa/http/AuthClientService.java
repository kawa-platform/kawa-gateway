package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import org.jspecify.annotations.NullUnmarked;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/// Owns the auth client section of the gateway config snapshot, including group
/// membership synchronization. Every write honours [Ownership]: a managed client, and a managed
/// group whose membership a client write would change, only change through their owner.
final class AuthClientService {

    private static final String KIND = "client";

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    AuthClientService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, ClientConfig> listClients() {
        return repository.getActiveConfigOrEmpty().auth().clients();
    }

    ClientConfig upsertClient(KawaPrincipal caller, String name, ClientConfigRequest request, Consistency consistency) {
        ClientConfig client = ClientConfig.fromPlaintext(request.mechanism(), request.password()).withManagedBy(Ownership.stamp(caller));
        List<String> groups = request.groups() == null ? List.of() : request.groups();
        updater.update(consistency, config -> updateClient(config, caller, name, client, groups));
        return client;
    }

    ClientConfig updateClient(KawaPrincipal caller, String name, ClientConfigPatch patch, Consistency consistency) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        ClientConfig current = base.auth().clients().get(name);
        if (current == null) {
            throw new NotFoundException("client '" + name + "' not found");
        }
        ClientConfigRequest body = patch.toRequest();
        if (body.mechanism() == null && body.password() == null && body.groups() == null) {
            throw new IllegalArgumentException("no fields to patch");
        }
        String mechanism = body.mechanism() == null ? current.mechanism() : body.mechanism();
        HashedPassword password = body.password() == null
                ? current.password()
                : HashedPassword.fromPlaintext(Mechanism.fromWireName(mechanism), body.password());
        ClientConfig client = new ClientConfig(mechanism, password, Ownership.stamp(caller));
        updater.update(consistency, config -> updateClient(config, caller, name, client, body.groups()));
        return client;
    }

    void deleteClient(KawaPrincipal caller, String name, Consistency consistency) {
        if (!repository.getActiveConfigOrEmpty().auth().clients().containsKey(name)) {
            throw new NotFoundException("client '" + name + "' not found");
        }
        updater.update(consistency, config -> {
            requireClientOwner(config, name, caller);
            config.rbac().groups().forEach((groupName, group) -> {
                if (group.clients().contains(name)) {
                    Ownership.requireOwner("group", groupName, group.managedBy(), caller);
                }
            });
            var groups = config.rbac().groups().entrySet().stream().collect(
                    Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().removeClient(name)));
            return config.updateAuth(config.auth().removeClient(name))
                    .updateRbac(new RbacConfig(config.rbac().roles(), groups));
        });
    }

    /// Clears the client's owner. Only the owner may release it; an unmanaged client is left as is.
    void releaseClient(KawaPrincipal caller, String name, Consistency consistency) {
        ClientConfig current = repository.getActiveConfigOrEmpty().auth().clients().get(name);
        if (current == null) {
            throw new NotFoundException("client '" + name + "' not found");
        }
        if (current.managedBy() == null) {
            return;
        }
        updater.update(consistency, config -> {
            ClientConfig client = config.auth().clients().get(name);
            if (client == null) {
                throw new NotFoundException("client '" + name + "' not found");
            }
            Ownership.requireOwner(KIND, name, client.managedBy(), caller);
            return config.updateAuth(config.auth().upsertClient(name, client.withManagedBy(null)));
        });
    }

    /// Writes `client` and makes it a member of exactly `groupNames` (its current groups when
    /// `null`). Refused when the client, or a group whose membership changes, is managed by someone
    /// other than `caller`.
    @NullUnmarked
    private GatewayConfig updateClient(
            GatewayConfig config,
            KawaPrincipal caller,
            String name,
            ClientConfig client,
            List<String> groupNames
    ) {
        requireClientOwner(config, name, caller);
        if (groupNames == null) {
            groupNames = groupsForClient(config, name);
        }
        Set<String> selectedGroups = new HashSet<>(groupNames);
        for (String groupName : selectedGroups) {
            if (!config.rbac().groups().containsKey(groupName)) {
                throw new IllegalArgumentException("group '" + groupName + "' not found");
            }
        }
        config.rbac().groups().forEach((groupName, group) -> {
            if (group.clients().contains(name) != selectedGroups.contains(groupName)) {
                Ownership.requireOwner("group", groupName, group.managedBy(), caller);
            }
        });
        var groups = config.rbac().groups().entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            List<String> clients = entry.getValue().clients().stream()
                                    .filter(clientName -> !clientName.equals(name))
                                    .collect(Collectors.toCollection(ArrayList::new));
                            if (selectedGroups.contains(entry.getKey())) {
                                clients.add(name);
                            }
                            return entry.getValue().withClients(clients);
                        }));
        return config.updateAuth(config.auth().upsertClient(name, client))
                .updateRbac(new RbacConfig(config.rbac().roles(), groups));
    }

    private static void requireClientOwner(GatewayConfig config, String name, KawaPrincipal caller) {
        ClientConfig existing = config.auth().clients().get(name);
        if (existing != null) {
            Ownership.requireOwner(KIND, name, existing.managedBy(), caller);
        }
    }

    private List<String> groupsForClient(GatewayConfig config, String name) {
        return config.rbac().groups().entrySet().stream()
                .filter(entry -> entry.getValue().clients().contains(name))
                .map(Map.Entry::getKey)
                .toList();
    }
}
