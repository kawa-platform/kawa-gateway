package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PATCH /rbac/groups/{name}`.
public final class PatchRbacGroupHandler implements Router.Handler {

    private final RbacGroupService service;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PatchRbacGroupHandler(RbacGroupService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        String name = request.pathParams().get("name");
        GroupConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), GroupConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid group body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var value = service.updateGroup(name, patch, consistency);
            String responseName = patch.name() == null ? name : patch.name();
            return Router.Response.ok(new GroupConfigView(responseName, value.clients(), value.roles()));
        } catch (ConflictException e) {
            return Router.Response.conflict(e.getMessage());
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
