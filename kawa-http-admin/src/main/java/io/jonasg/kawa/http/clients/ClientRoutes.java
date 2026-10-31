package io.jonasg.kawa.http.clients;

import io.jonasg.kawa.http.configchange.DeleteManagedByHandler;
import io.jonasg.kawa.http.web.Router;

/// Registers the `/auth/clients` routes, backed by the [AuthClientService], on a [Router].
public final class ClientRoutes {

    private ClientRoutes() {
    }

    public static void register(Router router, AuthClientService clientService) {
        router.get("/auth/clients", new GetAuthClientsHandler(clientService))
                .put("/auth/clients/{name}", new PutAuthClientHandler(clientService))
                .patch("/auth/clients/{name}", new PatchAuthClientHandler(clientService))
                .delete("/auth/clients/{name}", new DeleteAuthClientHandler(clientService))
                .delete("/auth/clients/{name}/managed-by", new DeleteManagedByHandler(clientService::releaseClient));
    }
}
