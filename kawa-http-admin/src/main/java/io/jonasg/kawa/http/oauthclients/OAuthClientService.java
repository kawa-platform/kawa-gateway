package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ConsistencyAwareUpdater;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.configchange.Ownership;

import java.util.Collection;
import java.util.Map;

/// Owns reading and writing the OAuth clients section of the gateway config snapshot.
///
/// The built-in clients are configured with `admin.auth` and never stored here: `kawa-studio`, the client of kawa's
/// own UI, and `kawa-operator`, the Kubernetes operator. Their ids are reserved, and writing or deleting them is
/// refused with a [ConflictException].
public final class OAuthClientService {

    private static final Map<String, String> RESERVED_IDS = Map.of(
            "kawa-studio", "kawa studio, which is configured with admin.auth",
            Ownership.OPERATOR, "the kawa operator, which is configured with admin.auth.settings.operatorSecret");

    private final DynamicConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    public OAuthClientService(DynamicConfigRepository repository) {
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
        String owner = RESERVED_IDS.get(id);
        if (owner != null) {
            throw new ConflictException("OAuth client id '" + id + "' is reserved for " + owner);
        }
    }

    private static NotFoundException notFound(String id) {
        return new NotFoundException("OAuth client '" + id + "' not found");
    }
}
