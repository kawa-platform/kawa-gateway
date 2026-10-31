package io.jonasg.kawa.http.clients;

import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.http.web.Router;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/// Serves `GET /auth/clients`.
final class GetAuthClientsHandler implements Router.Handler {

    private final AuthClientService service;

    GetAuthClientsHandler(AuthClientService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        Map<String, ClientConfig> clients = service.listClients();
        List<ClientConfigView> views = clients.entrySet().stream()
                .map(entry -> new ClientConfigView(entry.getKey(), entry.getValue().mechanism(), entry.getValue().managedBy()))
                .sorted(Comparator.comparing(ClientConfigView::username))
                .toList();
        return Router.Response.ok(views);
    }
}
