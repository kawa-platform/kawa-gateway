package io.jonasg.kawa.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    private final ConfigLoader loader = new ConfigLoader();

    @Test
    void loadsFullConfiguration() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                listeners:
                  - host: 0.0.0.0
                    port: 9092
                clusters:
                  default:
                    bootstrapServers:
                      - localhost:9093
                      - localhost:9094
                advertised:
                  nodeId: 7
                  host: gw.example.com
                  port: 9092
                """);

        // then
        assertThat(config.listeners()).containsExactly(new ListenerConfig("0.0.0.0", 9092));
        assertThat(config.defaultCluster().bootstrapServers()).containsExactly("localhost:9093", "localhost:9094");
        assertThat(config.advertised()).isEqualTo(new AdvertisedListener(7, "gw.example.com", 9092));
    }

    @Test
    void appliesDefaults() {
        // when
        BootstrapConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        // then
        assertThat(config.listeners()).containsExactly(new ListenerConfig("0.0.0.0", 9092));
        assertThat(config.clusters()).isEmpty();
        assertThat(config.advertised()).isEqualTo(new AdvertisedListener(1, "localhost", 9092));
        assertThat(config.auth().brokerAuth()).isNull();
        assertThat(config.admin().enabled()).isFalse();
        assertThat(config.configTopic()).isEqualTo("__kawa");
    }

    @Test
    void loadsConfigTopic() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                configTopic: kawa-config
                listeners:
                  - port: 9092
                """);

        // then
        assertThat(config.configTopic()).isEqualTo("kawa-config");
    }

    @Test
    void rejectsUnknownKeys() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                listener:
                  - port: 9092
                """))
                .withFailMessage(() -> "The misspelled 'listener' key was not rejected")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("listener");
    }

    @Test
    void rejectsUnknownNestedKeys() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                admin:
                  enabled: true
                  prot: 8081
                """))
                .withFailMessage(() -> "The misspelled 'admin.prot' key was not rejected")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prot");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "virtualTopics:\n  orders: orders-v2\n",
        "rbac:\n  roles: {}\n",
        "governance:\n  rules: {}\n",
        "adminUsers: {}\n",
        "modules: {}\n",
        "auth:\n  mechanisms: [PLAIN]\n",
        "auth:\n  clients:\n    alice:\n      mechanism: PLAIN\n      password: s3cret\n",
    })
    void rejectsDynamicSections(String yaml) {
        assertThatThrownBy(() -> loader.loadFromYaml(yaml))
                .withFailMessage(() -> "The dynamic section was accepted in the bootstrap file:\n" + yaml)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unrecognized property");
    }

    @Test
    void loadsBrokerAuthConfiguration() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                auth:
                  brokerAuth:
                    mechanism: PLAIN
                    username: gateway-service
                    password: s3cret
                listeners:
                  - port: 9092
                """);

        // then
        assertThat(config.auth().brokerAuth()).isNotNull();
        assertThat(config.auth().brokerAuth().mechanism()).isEqualTo("PLAIN");
        assertThat(config.auth().brokerAuth().username()).isEqualTo("gateway-service");
        assertThat(config.auth().brokerAuth().password()).isEqualTo("s3cret");
    }

    @Test
    void loadsAdminConfiguration() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  host: 127.0.0.1
                  port: 8081
                """);

        // then
        assertThat(config.admin().enabled()).isTrue();
        assertThat(config.admin().host()).isEqualTo("127.0.0.1");
        assertThat(config.admin().port()).isEqualTo(8081);
        assertThat(config.admin().cors()).isNull();
    }

    @Test
    void loadsAdminCorsConfiguration() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  host: 127.0.0.1
                  port: 8081
                  cors:
                    allowedOrigins:
                      - http://localhost:8080
                      - http://localhost:5173
                    allowedMethods:
                      - GET
                      - OPTIONS
                    allowedHeaders:
                      - Content-Type
                    allowCredentials: true
                    maxAge: 3600
                """);

        // then
        CorsConfig cors = config.admin().cors();
        assertThat(cors).isNotNull();
        assertThat(cors.allowedOrigins())
                .containsExactly("http://localhost:8080", "http://localhost:5173");
        assertThat(cors.allowedMethods()).containsExactly("GET", "OPTIONS");
        assertThat(cors.allowedHeaders()).containsExactly("Content-Type");
        assertThat(cors.allowCredentials()).isTrue();
        assertThat(cors.maxAge()).isEqualTo(3600L);
    }

    @Test
    void adminCorsDefaultsToDisabled() {
        // when
        BootstrapConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        // then
        assertThat(config.admin().enabled()).isFalse();
        assertThat(config.admin().host()).isEqualTo("0.0.0.0");
        assertThat(config.admin().port()).isEqualTo(8080);
        assertThat(config.admin().cors()).isNull();
    }

    /// The SASL_SSL sample is left out: its stores only exist inside the compose setup.
    @ParameterizedTest
    @ValueSource(strings = {"gateway.yaml", "gateway-sasl.yaml"})
    void loadsTheDockerSamples(String file) {
        // given
        Path sample = Path.of("..", "docker", file);

        // when
        BootstrapConfig config = loader.load(sample);

        // then
        assertThat(config.defaultCluster())
                .withFailMessage(() -> "The sample " + sample + " did not load its cluster")
                .isNotNull();
    }

    @Test
    void namesTheFileWhenItIsInvalid(@TempDir Path dir) throws IOException {
        // given
        Path file = Files.writeString(dir.resolve("gateway.yaml"), "rbac:\n  roles: {}\n");

        // when / then
        assertThatThrownBy(() -> loader.load(file))
                .withFailMessage(() -> "The error did not name the invalid file " + file)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(file.toString())
                .hasMessageContaining("rbac");
    }
}
