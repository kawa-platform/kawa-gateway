package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.BootstrapConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.SchemaRegistryConfig;
import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.jonasg.kawa.config.TlsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaRegistryStartupTest {

    @Test
    void refusesToStartWhenSchemaRegistryTlsIsSet(@TempDir Path dir) throws IOException {
        // given a cluster whose schema registry has TLS settings, which are not supported yet
        Path truststore = Files.createFile(dir.resolve("registry-truststore.p12"));
        var tls = new TlsConfig(new StoreConfig(truststore.toString(), StoreType.PKCS12, "changeit", null), null, true);
        var registry = new SchemaRegistryConfig("https://registry:8081", null, tls);
        var gateway = new KafkaGateway(config(new ClusterConfig(List.of("localhost:9092"), null, registry)));

        // when / then
        assertThatThrownBy(gateway::start)
                .withFailMessage(() -> "The gateway started although schemaRegistry.tls is set")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("schemaRegistry.tls is not supported yet");
    }

    private static BootstrapConfig config(ClusterConfig cluster) {
        return new BootstrapConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", cluster),
                new AdvertisedListener(1, "localhost", 0),
                null,
                null,
                "__kawa");
    }
}
