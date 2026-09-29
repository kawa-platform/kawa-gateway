package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ClientConfig;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/// Serves `GET /auth/clients`.
public final class GetAuthClientsHandler implements Router.Handler {

    private final AuthClientService service;

    public GetAuthClientsHandler(AuthClientService service) {
        this.service = service;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        Map<String, ClientConfig> clients = service.listClients();
        List<ClientConfigView> views = clients.entrySet().stream()
                .map(entry -> new ClientConfigView(entry.getKey(), entry.getValue().mechanism()))
                .sorted(Comparator.comparing(ClientConfigView::username))
                .toList();
        return Router.Response.ok(views);
    }
}
