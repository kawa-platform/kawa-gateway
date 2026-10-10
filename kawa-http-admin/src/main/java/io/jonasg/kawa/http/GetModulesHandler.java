package io.jonasg.kawa.http;

import java.util.Comparator;

/// Serves `GET /modules`: every stored module, sorted by id.
public final class GetModulesHandler implements Router.Handler {

    private final ModuleService service;
    private final ModuleConfigMapper mapper;

    GetModulesHandler(ModuleService service, ModuleConfigMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(service.listModules().stream()
                .map(mapper::toModuleConfigView)
                .sorted(Comparator.comparing(ModuleConfigView::id))
                .toList());
    }
}
