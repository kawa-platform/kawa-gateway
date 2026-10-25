package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.util.List;

/// A Kafka cluster the gateway can forward traffic to.
///
/// @param bootstrapServers `host:port` list used to open the initial broker connection
/// @param tls              TLS for the connections to the cluster's brokers, or `null` to connect
///                         without TLS
/// @param schemaRegistry   the schema registry for virtual topics whose values are decoded by
///                         schema, or `null` for none
public record ClusterConfig(
        List<String> bootstrapServers,
        @Nullable TlsConfig tls,
        @Nullable SchemaRegistryConfig schemaRegistry
) {

    public ClusterConfig {
        if (bootstrapServers == null) {
            bootstrapServers = List.of();
        }
    }

    public ClusterConfig(List<String> bootstrapServers) {
        this(bootstrapServers, null, null);
    }

    public ClusterConfig(List<String> bootstrapServers, @Nullable TlsConfig tls) {
        this(bootstrapServers, tls, null);
    }

    public static ClusterConfig of(
            List<String> bootstrapServers
    ) {
        return new ClusterConfig(bootstrapServers);
    }
}
