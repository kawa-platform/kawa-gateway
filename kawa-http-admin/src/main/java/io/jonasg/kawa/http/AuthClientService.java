package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.RbacConfig;
import org.jspecify.annotations.NullUnmarked;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/// Owns the auth client section of the gateway config snapshot, including group
/// membership synchronization.
final class AuthClientService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    AuthClientService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Map<String, ClientConfig> listClients() {
        return repository.getActiveConfigOrEmpty().auth().clients();
    }

    ClientConfig upsertClient(String name, ClientConfigRequest request, Consistency consistency) {
        ClientConfig client = ClientConfig.fromPlaintext(request.mechanism(), request.password());
        List<String> groups = request.groups() == null ? List.of() : request.groups();
        updater.update(consistency, config -> updateClient(config, name, client, groups));
        return client;
    }

    ClientConfig updateClient(String name, ClientConfigPatch patch, Consistency consistency) {
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
        ClientConfig client = new ClientConfig(mechanism, password);
        updater.update(consistency, config -> updateClient(config, name, client, body.groups()));
        return client;
    }

    void deleteClient(String name, Consistency consistency) {
        if (!repository.getActiveConfigOrEmpty().auth().clients().containsKey(name)) {
            throw new NotFoundException("client '" + name + "' not found");
        }
        updater.update(consistency, config -> {
            var groups = config.rbac().groups().entrySet().stream().collect(
                    Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().removeClient(name)));
            return config.updateAuth(config.auth().removeClient(name))
                    .updateRbac(new RbacConfig(config.rbac().roles(), groups));
        });
    }

    @NullUnmarked
    private GatewayConfig updateClient(
            GatewayConfig config,
            String name,
            ClientConfig client,
            List<String> groupNames
    ) {
        if (groupNames == null) {
            groupNames = groupsForClient(config, name);
        }
        Set<String> selectedGroups = new HashSet<>(groupNames);
        for (String groupName : selectedGroups) {
            if (!config.rbac().groups().containsKey(groupName)) {
                throw new IllegalArgumentException("group '" + groupName + "' not found");
            }
        }
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
                            return new GroupConfig(clients, entry.getValue().roles());
                        }));
        return config.updateAuth(config.auth().upsertClient(name, client))
                .updateRbac(new RbacConfig(config.rbac().roles(), groups));
    }

    private List<String> groupsForClient(GatewayConfig config, String name) {
        return config.rbac().groups().entrySet().stream()
                .filter(entry -> entry.getValue().clients().contains(name))
                .map(Map.Entry::getKey)
                .toList();
    }
}
