package io.jonasg.kawa.http;

/// Serves `GET /modules/{id}`: one stored module.
public final class GetModuleHandler implements Router.Handler {

    private final ModuleService service;
    private final ModuleConfigMapper mapper;

    GetModuleHandler(ModuleService service, ModuleConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        try {
            var id = mapper.toModuleId(request.pathParams().get("id"));
            return Router.Response.ok(mapper.toModuleConfigView(service.getModule(id)));
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        }
    }
}
