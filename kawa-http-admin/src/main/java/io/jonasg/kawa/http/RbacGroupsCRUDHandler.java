package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;

import java.util.Comparator;
import java.util.Map;

final class RbacGroupsCRUDHandler extends BaseCRUDHandler<GroupConfig, GroupConfig> {

    RbacGroupsCRUDHandler(GatewayConfigRepository repository) {
        super(repository, GroupConfig.class, "group");
    }

    @Override
    protected Map<String, GroupConfig> entries(GatewayConfig config) {
        return config.rbac().groups();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new GroupView(entry.getKey(), entry.getValue().clients(), entry.getValue().roles()))
                .sorted(Comparator.comparing(GroupView::name))
                .toList();
    }

    @Override
    protected GroupConfig toConfig(String name, GroupConfig body) {
        return body;
    }

    @Override
    protected Object putView(String name, GroupConfig value) {
        return value;
    }

    @Override
    protected Router.Response<?> validateRemove(GatewayConfig config, String name) {
        GroupConfig group = entries(config).get(name);
        if (group.clients().isEmpty()) {
            return null;
        }
        return Router.Response.conflict("group '" + name + "' still has clients " + group.clients());
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, GroupConfig value) {
        return config.updateRbac(config.rbac().upsertGroup(name, value));
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateRbac(config.rbac().removeGroup(name));
    }

    Router.Response<?> patch(Router.Request request) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        GroupConfig current = entries(base).get(name);
        if (current == null) {
            return Router.Response.notFound("group '" + name + "' not found");
        }
        GroupConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), GroupConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid group body: " + e.getMessage());
        }
        String newName = patch.name();
        if (newName == null || newName.isBlank()) {
            return Router.Response.badRequest("no new name to rename to");
        }
        if (newName.equals(name)) {
            return Router.Response.badRequest("group is already named '" + name + "'");
        }
        if (entries(base).containsKey(newName)) {
            return Router.Response.conflict("group '" + newName + "' already exists");
        }
        try {
            updater.update(request, config -> config.updateRbac(config.rbac().renameGroup(name, newName)));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(new GroupView(newName, current.clients(), current.roles()));
    }
}
