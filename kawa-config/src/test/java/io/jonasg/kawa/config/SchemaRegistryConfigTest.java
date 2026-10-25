package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaRegistryConfigTest {

    @TempDir
    Path dir;

    private final ConfigLoader loader = new ConfigLoader();
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void loadsAClustersSchemaRegistry() throws IOException {
        // given
        String ca = Files.writeString(dir.resolve("ca.pem"), "not read by these tests").toString();

        // when
        BootstrapConfig config = loader.loadFromYaml("""
                clusters:
                  default:
                    bootstrapServers: [kafka:9092]
                    schemaRegistry:
                      url: https://registry:8081
                      basicAuth:
                        username: kawa
                        password: s3cret
                      tls:
                        truststore: {location: %s, type: PEM}
                """.formatted(ca));

        // then
        SchemaRegistryConfig registry = config.defaultCluster().schemaRegistry();
        assertThat(registry)
                .withFailMessage(() -> "The cluster's schemaRegistry block was not loaded")
                .isEqualTo(new SchemaRegistryConfig(
                        "https://registry:8081",
                        new SchemaRegistryBasicAuthConfig("kawa", "s3cret"),
                        new TlsConfig(new StoreConfig(ca, StoreType.PEM, null, null), null, true)));
    }

    @Test
    void acceptsARegistryWithoutAuthOrTls() {
        // when
        var registry = new SchemaRegistryConfig("http://registry:8081", null, null);

        // then
        assertThat(registry.basicAuth()).isNull();
        assertThat(registry.tls()).isNull();
    }

    @Test
    void refusesAMissingUrl() {
        assertThatThrownBy(() -> new SchemaRegistryConfig(null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaRegistry.url is required");
    }

    @Test
    void refusesAUrlThatIsNotAbsoluteHttp() {
        for (String url : List.of("registry:8081", "/subjects", "ftp://registry:8081", "http://")) {
            assertThatThrownBy(() -> new SchemaRegistryConfig(url, null, null))
                    .withFailMessage(() -> "The url '" + url + "' was accepted")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("schemaRegistry.url");
        }
    }

    @Test
    void refusesTlsOnAnHttpUrl() throws IOException {
        var tls = new TlsConfig(
                new StoreConfig(Files.writeString(dir.resolve("ca.pem"), "x").toString(), StoreType.PEM, null, null), null, null);

        assertThatThrownBy(() -> new SchemaRegistryConfig("http://registry:8081", null, tls))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("use https");
    }

    @Test
    void refusesBasicAuthWithoutAPassword() {
        assertThatThrownBy(() -> new SchemaRegistryBasicAuthConfig("kawa", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("basicAuth.password is required");
    }

    @Test
    void resolvesEnvironmentVariablesInThePassword() {
        // when
        var auth = new SchemaRegistryBasicAuthConfig("kawa", "${KAWA_TEST_UNSET_SR_PASSWORD:-fallback}");

        // then
        assertThat(auth.password()).isEqualTo("fallback");
        assertThat(auth.toString())
                .withFailMessage(() -> "The basic auth password was printed: " + auth)
                .doesNotContain("fallback");
    }

    @Test
    void roundTripsAnAvroValueFormatThroughTheConfigTopic() throws Exception {
        // given
        var virtualTopic = new VirtualTopicConfig("orders", new CelFilterConfig("value.region == \"eu\""), false,
                new AvroFormatConfig(DecodeErrorPolicy.INCLUDE));
        var config = DynamicConfig.empty().upsertVirtualTopic("orders.eu", virtualTopic);

        // when
        String json = mapper.writeValueAsString(config);

        // then
        assertThat(mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.eu"))
                .withFailMessage(() -> "The Avro valueFormat did not survive the config-topic round trip: " + json)
                .isEqualTo(virtualTopic);
    }
}
