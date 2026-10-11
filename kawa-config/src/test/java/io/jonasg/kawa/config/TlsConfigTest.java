package io.jonasg.kawa.config;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TlsConfigTest {

    @TempDir
    Path dir;

    private final ConfigLoader loader = new ConfigLoader();

    private String file(String name) throws IOException {
        return Files.writeString(dir.resolve(name), "not read by these tests").toString();
    }

    @Test
    void loadsAClustersTlsWithDefaults() throws IOException {
        // given
        String truststore = file("truststore.p12");

        // when
        GatewayConfig config = loader.loadFromYaml("""
                clusters:
                  default:
                    bootstrapServers: [kafka:9093]
                    tls:
                      truststore:
                        location: %s
                        password: changeit
                """.formatted(truststore));

        // then
        TlsConfig tls = config.defaultCluster().tls();
        assertThat(tls)
                .withFailMessage(() -> "The cluster's tls block was not loaded")
                .isNotNull();
        assertThat(tls.truststore())
                .withFailMessage(() -> "Expected a PKCS12 truststore by default, got " + tls.truststore())
                .isEqualTo(new StoreConfig(truststore, StoreType.PKCS12, "changeit", null));
        assertThat(tls.keystore()).isNull();
        assertThat(tls.verifyHostname())
                .withFailMessage(() -> "Hostname verification is not on by default")
                .isTrue();
    }

    @Test
    void loadsPemStoresAndAKeystore() throws IOException {
        // given
        String ca = file("ca.pem");
        String gateway = file("gateway.pem");

        // when
        GatewayConfig config = loader.loadFromYaml("""
                clusters:
                  default:
                    bootstrapServers: [kafka:9093]
                    tls:
                      truststore: {location: %s, type: PEM}
                      keystore: {location: %s, type: PEM, keyPassword: k3y}
                      verifyHostname: false
                """.formatted(ca, gateway));

        // then
        TlsConfig tls = config.defaultCluster().tls();
        assertThat(tls).isEqualTo(new TlsConfig(
                new StoreConfig(ca, StoreType.PEM, null, null),
                new StoreConfig(gateway, StoreType.PEM, null, "k3y"),
                false));
    }

    @Test
    void refusesATlsBlockWithoutATruststore() {
        assertThatThrownBy(() -> new TlsConfig(null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tls.truststore is required");
    }

    @Test
    void refusesAStoreThatDoesNotExist() {
        assertThatThrownBy(() -> new StoreConfig(dir.resolve("missing.p12").toString(), StoreType.PKCS12, "changeit", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void refusesAPkcs12OrJksStoreWithoutAPassword() throws IOException {
        String store = file("store.jks");

        assertThatThrownBy(() -> new StoreConfig(store, StoreType.JKS, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a password is required for JKS");
    }

    @Test
    void refusesAStorePasswordOnAPemFile() throws IOException {
        String pem = file("gateway.pem");

        assertThatThrownBy(() -> new StoreConfig(pem, StoreType.PEM, "changeit", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("use keyPassword");
    }

    @Test
    void neverWritesTlsToTheConfigTopic() throws IOException {
        // given
        var tls = new TlsConfig(new StoreConfig(file("truststore.p12"), StoreType.PKCS12, "s3cret-store", null), null, null);
        var config = GatewayConfig.of(null, java.util.Map.of("default", new ClusterConfig(List.of("kafka:9093"), tls)),
                null, null, null);

        // when
        String json = JsonMapper.builder().build().writeValueAsString(config);

        // then
        assertThat(json)
                .withFailMessage(() -> "The cluster's TLS settings, with their passwords, were serialized: " + json)
                .doesNotContain("truststore")
                .doesNotContain("s3cret-store");
    }

    @Test
    void derivesTheSecurityProtocolFromTlsAndBrokerAuth() throws IOException {
        // given
        var tls = new TlsConfig(new StoreConfig(file("ca.pem"), StoreType.PEM, null, null), null, null);
        var auth = new BrokerAuthConfig("PLAIN", "gateway", "s3cret");

        // when / then
        assertThat(List.of(
                BrokerSecurity.protocol(null, null),
                BrokerSecurity.protocol(null, auth),
                BrokerSecurity.protocol(tls, null),
                BrokerSecurity.protocol(tls, auth)))
                .containsExactly(SecurityProtocol.PLAINTEXT, SecurityProtocol.SASL_PLAINTEXT,
                        SecurityProtocol.SSL, SecurityProtocol.SASL_SSL);
    }

    @Test
    void passesTheStoresToKafkaClients() throws IOException {
        // given
        String truststore = file("truststore.p12");
        String keystore = file("gateway.pem");
        var tls = new TlsConfig(
                new StoreConfig(truststore, StoreType.PKCS12, "changeit", null),
                new StoreConfig(keystore, StoreType.PEM, null, "k3y"),
                false);

        // when
        var props = BrokerSecurity.clientProperties(tls, new BrokerAuthConfig("PLAIN", "gateway", "s3cret"));

        // then
        assertThat(props)
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "PLAIN")
                .containsEntry("ssl.truststore.location", truststore)
                .containsEntry("ssl.truststore.type", "PKCS12")
                .containsEntry("ssl.truststore.password", "changeit")
                .containsEntry("ssl.keystore.location", keystore)
                .containsEntry("ssl.keystore.type", "PEM")
                .containsEntry("ssl.key.password", "k3y")
                .containsEntry("ssl.endpoint.identification.algorithm", "")
                .doesNotContainKey("ssl.keystore.password");
    }

    @Test
    void connectsKafkaClientsInPlaintextWithoutTlsOrBrokerAuth() {
        assertThat(BrokerSecurity.clientProperties(null, null))
                .containsExactly(java.util.Map.entry("security.protocol", "PLAINTEXT"));
    }
}
