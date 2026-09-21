package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.RbacConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class AuthClientsCRUDHandler extends BaseCRUDHandler<ClientConfig> {

    AuthClientsCRUDHandler(GatewayConfigRepository repository) {
        super(repository, ClientConfig.class, "client");
    }

    @Override
    protected Map<String, ClientConfig> entries(GatewayConfig config) {
        return config.auth().clients();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new ClientView(entry.getKey(), entry.getValue().mechanism()))
                .sorted(Comparator.comparing(ClientView::username))
                .toList();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, ClientConfig value) {
        AuthConfig auth = config.auth().upsertClient(name, value);
        return config.updateAuth(auth);
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        var groups = config.rbac().groups().entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().removeClient(name)));
        return config.updateAuth(config.auth().removeClient(name))
                .updateRbac(new RbacConfig(config.rbac().roles(), groups));
    }

    Router.Response<?> patch(Router.Request request) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        ClientConfig current = entries(base).get(name);
        ClientConfigRequest body;
        try {
            body = mapper.readValue(request.body(), ClientConfigPatch.class).toRequest();
        } catch (Exception e) {
            return Router.Response.badRequest("invalid client body: " + e.getMessage());
        }
        if (current == null) {
            return Router.Response.notFound("client '" + name + "' not found");
        }
        if (body.mechanism() == null && body.password() == null && body.groups() == null) {
            return Router.Response.badRequest("no fields to patch");
        }
        String mechanism = body.mechanism() == null ? current.mechanism() : body.mechanism();
        try {
            HashedPassword password = body.password() == null
                    ? current.password()
                    : HashedPassword.fromPlaintext(Mechanism.fromWireName(mechanism), body.password());
            updater.update(request, config -> updateClient(
                    config, name, new ClientConfig(mechanism, password), body.groups()));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(new ClientView(name, mechanism));
    }

    @Override
    Router.Response<?> put(Router.Request request) {
        String name = request.pathParams().get("name");
        ClientConfigRequest body;
        try {
            body = mapper.readValue(request.body(), ClientConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid client body: " + e.getMessage());
        }
        try {
            updater.update(request, config -> updateClient(
                    config, name, ClientConfig.fromPlaintext(body.mechanism(), body.password()),
                    body.groups() == null ? List.of() : body.groups()));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(new ClientView(name, body.mechanism()));
    }

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
