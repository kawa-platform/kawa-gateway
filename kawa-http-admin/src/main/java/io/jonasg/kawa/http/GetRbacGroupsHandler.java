package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GroupConfig;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/// Serves `GET /rbac/groups`.
public final class GetRbacGroupsHandler implements Router.Handler {

    private final RbacGroupService service;

    public GetRbacGroupsHandler(RbacGroupService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        Map<String, GroupConfig> groups = service.listGroups();
        List<GroupConfigView> views = groups.entrySet().stream()
                .map(entry -> new GroupConfigView(entry.getKey(), entry.getValue().clients(), entry.getValue().roles()))
                .sorted(Comparator.comparing(GroupConfigView::name))
                .toList();
        return Router.Response.ok(views);
    }
}
