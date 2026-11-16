package io.jonasg.kawa.http.modules;

import io.jonasg.kawa.http.web.Router;
import org.jspecify.annotations.Nullable;

/// Registers the `/modules` routes, backed by the [ModuleService], on a [Router].
public final class ModuleRoutes {

    private ModuleRoutes() {
    }

    public static void register(Router router, ModuleService moduleService, @Nullable String publicUrl) {
        var mapper = new ModuleConfigMapper(publicUrl == null ? null : "%s/mcp".formatted(publicUrl));
        router.get("/modules", new GetModulesHandler(moduleService, mapper))
                .get("/modules/{id}", new GetModuleHandler(moduleService, mapper))
                .put("/modules/{id}", new PutModuleHandler(moduleService, mapper));
    }
}
