package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ConsistencyAwareUpdater;
import io.jonasg.kawa.http.configchange.NotFoundException;

import java.util.Collection;

/// Owns reading and writing the OAuth clients section of the gateway config snapshot.
///
/// `kawa-studio`, the client of kawa's own UI, is configured with `admin.auth` and never stored here: its id is
/// reserved, and writing or deleting it is refused with a [ConflictException].
public final class OAuthClientService {

    static final String RESERVED_ID = "kawa-studio";

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    public OAuthClientService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    Collection<OAuthClientConfig> listOAuthClients() {
        return repository.getActiveConfigOrEmpty().oauthClients().values();
    }

    OAuthClientConfig getOAuthClient(String id) {
        OAuthClientConfig client = repository.getActiveConfigOrEmpty().oauthClients().get(id);
        if (client == null) {
            throw notFound(id);
        }
        return client;
    }

    /// Adds or replaces an OAuth client.
    OAuthClientConfig upsertOAuthClient(OAuthClientConfig client, Consistency consistency) {
        requireNotReserved(client.id());
        updater.update(consistency, config -> config.upsertOAuthClient(client));
        return client;
    }

    void deleteOAuthClient(String id, Consistency consistency) {
        requireNotReserved(id);
        updater.update(consistency, config -> {
            if (!config.oauthClients().containsKey(id)) {
                throw notFound(id);
            }
            return config.removeOAuthClient(id);
        });
    }

    private static void requireNotReserved(String id) {
        if (RESERVED_ID.equals(id)) {
            throw new ConflictException("OAuth client id '" + id + "' is reserved for kawa studio, which is configured with admin.auth");
        }
    }

    private static NotFoundException notFound(String id) {
        return new NotFoundException("OAuth client '" + id + "' not found");
    }
}
