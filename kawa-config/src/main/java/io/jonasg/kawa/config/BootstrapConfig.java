package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

/// The static bootstrap configuration, loaded once at startup from the `--config` YAML file by
/// [ConfigLoader]. Everything that changes at runtime (client auth, RBAC, virtual topics,
/// governance, admin users, modules) is [DynamicConfig] and lives in the config topic instead.
///
/// @param listeners   client-facing listeners
/// @param clusters    upstream Kafka clusters
/// @param advertised  the endpoint advertised to clients in rewritten metadata
/// @param auth        how the gateway authenticates to the brokers
/// @param admin       admin HTTP listener configuration
/// @param configTopic the topic that holds the dynamic gateway config (defaults to `__kawa`)
public record BootstrapConfig(
        List<ListenerConfig> listeners,
        Map<String, ClusterConfig> clusters,
        AdvertisedListener advertised,
        Auth auth,
        AdminConfig admin,
        String configTopic) {

    public BootstrapConfig {
        if (listeners == null) {
            listeners = List.of(new ListenerConfig(null, 9092));
        }
        if (clusters == null) {
            clusters = Map.of();
        }
        if (auth == null) {
            auth = new Auth(null);
        }
        if (admin == null) {
            admin = new AdminConfig(false, null, null, null);
        }
        if (configTopic == null) {
            configTopic = "__kawa";
        }
        if (advertised == null) {
            ListenerConfig first = listeners.getFirst();
            advertised = new AdvertisedListener(null, null, first.port());
        }
    }

    /// The default cluster (first entry), or `null` if none is configured.
    public @Nullable ClusterConfig defaultCluster() {
        if (clusters.isEmpty()) {
            return null;
        }
        return clusters.values().iterator().next();
    }

    /// The `auth` section of the bootstrap file.
    ///
    /// @param brokerAuth SASL credentials the gateway uses towards the brokers, or `null` when the
    ///                   brokers do not require SASL
    public record Auth(@Nullable BrokerAuthConfig brokerAuth) {
    }
}
