package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// A Kafka cluster the gateway can forward traffic to.
///
/// @param bootstrapServers `host:port` list used to open the initial broker connection
/// @param tls              TLS for the connections to the cluster's brokers, or `null` to connect
///                         without TLS. Startup-only and write-only: it holds store passwords, so it
///                         is never written to the config topic.
/// @param schemaRegistry   the schema registry for virtual topics whose values are decoded by
///                         schema, or `null` for none. Startup-only and write-only like `tls`: it
///                         holds credentials.
public record ClusterConfig(
        List<String> bootstrapServers,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) @Nullable TlsConfig tls,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) @Nullable SchemaRegistryConfig schemaRegistry
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
