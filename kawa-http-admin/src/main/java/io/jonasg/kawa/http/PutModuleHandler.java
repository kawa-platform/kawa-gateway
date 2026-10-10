package io.jonasg.kawa.http;

import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /modules/{id}`: adds or replaces a module. An unknown module id is a `404`.
public final class PutModuleHandler implements Router.Handler {

    private final ModuleService service;
    private final ModuleConfigMapper mapper;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    PutModuleHandler(ModuleService service, ModuleConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        ModuleRequest moduleReq;
        try {
            moduleReq = jsonMapper.readValue(request.body(), ModuleRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid module body: " + e.getMessage());
        }
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var id = mapper.toModuleId(request.pathParams().get("id"));
            var module = service.upsertModule(mapper.toModuleConfig(id, moduleReq), consistency);
            return Router.Response.ok(mapper.toModuleConfigView(module));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
