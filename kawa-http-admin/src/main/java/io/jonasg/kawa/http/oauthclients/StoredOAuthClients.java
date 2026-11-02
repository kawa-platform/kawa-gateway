package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.plugin.auth.OAuthClient;
import io.jonasg.kawa.plugin.auth.OAuthClientDirectory;

import java.util.Optional;

/// kawa's [OAuthClientDirectory]: the OAuth clients stored in the config topic, read from the active snapshot on
/// every call so changes made through `/admin/oauth-clients` apply immediately. Disabled clients are returned too;
/// the authentication provider decides what that means.
public final class StoredOAuthClients implements OAuthClientDirectory {

    private final DynamicConfigRepository repository;

    public StoredOAuthClients(DynamicConfigRepository repository) {
        this.repository = repository;
    }

    @Override
    public Optional<OAuthClient> find(String id) {
        return Optional.ofNullable(repository.getActiveConfigOrEmpty().oauthClients().get(id))
                .map(StoredOAuthClients::toOAuthClient);
    }

    private static OAuthClient toOAuthClient(OAuthClientConfig client) {
        var audience = switch (client.audience()) {
            case MCP -> OAuthClient.Audience.MCP;
            case ADMIN -> OAuthClient.Audience.ADMIN;
        };
        return new OAuthClient(client.id(), client.enabled(), client.redirectUris(), audience);
    }
}
