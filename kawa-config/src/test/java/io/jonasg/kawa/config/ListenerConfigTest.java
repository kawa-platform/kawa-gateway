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

class ListenerConfigTest {

    @TempDir
    Path dir;

    private final ConfigLoader loader = new ConfigLoader();

    private StoreConfig store(String name) throws IOException {
        String location = Files.writeString(dir.resolve(name), "not read by these tests").toString();
        return new StoreConfig(location, StoreType.PKCS12, "changeit", null);
    }

    @Test
    void defaultsToSaslPlaintextWithoutTls() {
        // when
        var listener = new ListenerConfig("0.0.0.0", 9092);

        // then
        assertThat(listener.securityProtocol())
                .withFailMessage(() -> "Expected SASL_PLAINTEXT by default, got " + listener.securityProtocol())
                .isEqualTo(SecurityProtocol.SASL_PLAINTEXT);
        assertThat(listener.tls()).isNull();
        assertThat(listener.usesSasl())
                .withFailMessage(() -> "The default listener does not require SASL")
                .isTrue();
    }

    @Test
    void loadsASaslSslListenerWithMutualTls() throws IOException {
        // given
        String keystore = store("gateway.p12").location();
        String truststore = store("truststore.p12").location();

        // when
        GatewayConfig config = loader.loadFromYaml("""
                listeners:
                  - port: 9093
                    securityProtocol: SASL_SSL
                    tls:
                      keystore: {location: %s, password: changeit}
                      truststore: {location: %s, password: changeit}
                      clientAuth: REQUIRED
                """.formatted(keystore, truststore));

        // then
        ListenerConfig listener = config.listeners().getFirst();
        assertThat(listener.securityProtocol()).isEqualTo(SecurityProtocol.SASL_SSL);
        assertThat(listener.tls()).isEqualTo(new ListenerTlsConfig(
                new StoreConfig(keystore, StoreType.PKCS12, "changeit", null),
                new StoreConfig(truststore, StoreType.PKCS12, "changeit", null),
                ClientAuth.REQUIRED,
                ListenerTlsConfig.DEFAULT_PRINCIPAL_MAPPING_RULES));
    }

    @Test
    void defaultsToNoClientAuthAndTheCnPrincipalRule() throws IOException {
        // when
        var tls = new ListenerTlsConfig(store("gateway.p12"), null, null, null);

        // then
        assertThat(tls.clientAuth())
                .withFailMessage(() -> "Expected no client auth by default, got " + tls.clientAuth())
                .isEqualTo(ClientAuth.NONE);
        assertThat(tls.principalMappingRules()).isEqualTo(ListenerTlsConfig.DEFAULT_PRINCIPAL_MAPPING_RULES);
    }

    @Test
    void refusesAnSslListenerWithoutTls() {
        assertThatThrownBy(() -> new ListenerConfig("0.0.0.0", 9093, SecurityProtocol.SSL, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uses SSL and needs a tls block");
    }

    @Test
    void refusesTlsOnAPlaintextListener() throws IOException {
        var tls = new ListenerTlsConfig(store("gateway.p12"), null, null, null);

        assertThatThrownBy(() -> new ListenerConfig("0.0.0.0", 9092, SecurityProtocol.SASL_PLAINTEXT, tls))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("use SASL_SSL for TLS");
    }

    @Test
    void refusesTlsWithoutAKeystore() throws IOException {
        var truststore = store("truststore.p12");

        assertThatThrownBy(() -> new ListenerTlsConfig(null, truststore, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tls.keystore is required");
    }

    @Test
    void refusesClientAuthWithoutATruststore() throws IOException {
        var keystore = store("gateway.p12");

        assertThatThrownBy(() -> new ListenerTlsConfig(keystore, null, ClientAuth.REQUESTED, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tls.truststore is required when clientAuth is REQUESTED");
    }

    @Test
    void refusesInvalidPrincipalMappingRules() throws IOException {
        var keystore = store("gateway.p12");

        assertThatThrownBy(() -> new ListenerTlsConfig(keystore, null, null, "NOT A RULE"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tls.principalMappingRules is invalid");
    }

    @Test
    void neverWritesListenerTlsToTheConfigTopic() throws IOException {
        // given
        var keystore = new StoreConfig(store("gateway.p12").location(), StoreType.PKCS12, "s3cret-store", null);
        var listener = new ListenerConfig("0.0.0.0", 9093, SecurityProtocol.SSL, new ListenerTlsConfig(keystore, null, null, null));
        var config = GatewayConfig.of(List.of(listener), null, null, null, null);

        // when
        String json = JsonMapper.builder().build().writeValueAsString(config);

        // then
        assertThat(json)
                .withFailMessage(() -> "The listener's TLS settings, with their passwords, were serialized: " + json)
                .doesNotContain("keystore")
                .doesNotContain("s3cret-store");
        assertThat(JsonMapper.builder().build().readValue(json, GatewayConfig.class).listeners())
                .withFailMessage(() -> "A snapshot of a TLS listener could not be read back: " + json)
                .hasSize(1);
    }
}
