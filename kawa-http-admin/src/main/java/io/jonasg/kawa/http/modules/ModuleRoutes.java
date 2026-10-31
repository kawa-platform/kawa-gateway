package io.jonasg.kawa.http.modules;

import io.jonasg.kawa.http.web.Router;

/// Registers the `/modules` routes, backed by the [ModuleService], on a [Router].
public final class ModuleRoutes {

    private ModuleRoutes() {
    }

    public static void register(Router router, ModuleService moduleService) {
        var mapper = new ModuleConfigMapper();
        router.get("/modules", new GetModulesHandler(moduleService, mapper))
                .get("/modules/{id}", new GetModuleHandler(moduleService, mapper))
                .put("/modules/{id}", new PutModuleHandler(moduleService, mapper));
    }
}
