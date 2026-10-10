package io.jonasg.kawa.http;

/// Serves `DELETE /modules/{id}`.
public final class DeleteModuleHandler implements Router.Handler {

    private final ModuleService service;
    private final ModuleConfigMapper mapper;

    DeleteModuleHandler(ModuleService service, ModuleConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        try {
            var consistency = Consistency.fromQueryParam(request.queryParams().get("consistency"));
            var id = mapper.toModuleId(request.pathParams().get("id"));
            service.deleteModule(id, consistency);
            return Router.Response.noContent();
        } catch (NotFoundException e) {
            return Router.Response.notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
    }
}
