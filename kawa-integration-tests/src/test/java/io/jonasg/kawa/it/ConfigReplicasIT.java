package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// Two gateway replicas sharing one config topic. Config writes are a compare-and-set across
/// replicas, so concurrent writes on different replicas never lose each other's changes, and the
/// admin user rails hold even when the two halves of a lockout reach different replicas.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigReplicasIT {

    private static final String CONFIG_TOPIC = "__kawa";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    private final HttpClient http = HttpClient.newHttpClient();
    private KafkaGateway first;
    private KafkaGateway second;

    @BeforeAll
    void setUp() throws Exception {
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }
        first = startReplica();
        second = startReplica();
    }

    @AfterAll
    void tearDown() {
        if (first != null) {
            first.stop();
        }
        if (second != null) {
            second.stop();
        }
    }

    @Test
    void concurrentWritesOnTwoReplicasLoseNothing() throws Exception {
        // given
        var start = new CountDownLatch(1);
        var writes = new ArrayList<CompletableFuture<HttpResponse<String>>>();
        for (int i = 0; i < 10; i++) {
            writes.add(async(start, first, "PUT", "/rbac/roles/first-" + i, "{\"acls\":[]}"));
            writes.add(async(start, second, "PUT", "/rbac/roles/second-" + i, "{\"acls\":[]}"));
        }

        // when
        start.countDown();

        // then
        for (var write : writes) {
            assertThat(write.get().statusCode())
                    .withFailMessage(() -> "a concurrent role write failed: " + write.join().body())
                    .isEqualTo(200);
        }
        List<String> expected = IntStream.range(0, 10)
                .boxed()
                .flatMap(i -> List.of("first-" + i, "second-" + i).stream())
                .toList();
        for (var replica : List.of(first, second)) {
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(names(send(replica, "GET", "/rbac/roles", null), "name"))
                            .withFailMessage(() -> "a replica lost a concurrent write")
                            .containsAll(expected));
        }
    }

    @Test
    void concurrentDeletesOnTwoReplicasKeepAnEnabledAdminUser() throws Exception {
        String previousSurvivor = null;
        for (int round = 0; round < 5; round++) {
            // given - exactly two enabled admin users, both visible to both replicas
            String alice = createAdminUser("alice-" + round + "@mycom.com");
            String bob = createAdminUser("bob-" + round + "@mycom.com");
            if (previousSurvivor != null) {
                assertThat(send(first, "DELETE", "/admin/users/" + previousSurvivor + "?consistency=applied", null).statusCode())
                        .isEqualTo(204);
            }
            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(names(send(second, "GET", "/admin/users", null), "id")).containsExactlyInAnyOrder(alice, bob));
            var start = new CountDownLatch(1);
            var deleteAlice = async(start, first, "DELETE", "/admin/users/" + alice, null);
            var deleteBob = async(start, second, "DELETE", "/admin/users/" + bob, null);

            // when - each replica deletes one of them at the same time
            start.countDown();

            // then - one delete wins, the other is refused as the last enabled admin user
            List<Integer> statuses = List.of(deleteAlice.get().statusCode(), deleteBob.get().statusCode());
            assertThat(statuses)
                    .withFailMessage(() -> "both replicas deleted an admin user; statuses " + statuses)
                    .containsExactlyInAnyOrder(204, 409);
            String survivor = deleteAlice.get().statusCode() == 204 ? bob : alice;
            for (var replica : List.of(first, second)) {
                Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                        assertThat(names(send(replica, "GET", "/admin/users", null), "id"))
                                .withFailMessage(() -> "the replicas did not keep exactly one enabled admin user")
                                .containsExactly(survivor));
            }
            previousSurvivor = survivor;
        }
    }

    private String createAdminUser(String email) throws Exception {
        var response = send(first, "POST", "/admin/users?consistency=applied",
                "{\"email\":\"" + email + "\",\"password\":\"secret\"}");
        assertThat(response.statusCode())
                .withFailMessage(() -> "creating admin user " + email + " failed: " + response.body())
                .isEqualTo(201);
        return JSON.readTree(response.body()).get("id").asString();
    }

    private KafkaGateway startReplica() throws Exception {
        var bootstrap = new GatewayConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(kafka.getBootstrapServers()))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new AuthConfig(null, null, null),
                null,
                new AdminConfig(true, "127.0.0.1", 0, null),
                CONFIG_TOPIC, null);
        var gateway = new KafkaGateway(bootstrap);
        gateway.start();
        return gateway;
    }

    private CompletableFuture<HttpResponse<String>> async(
            CountDownLatch start, KafkaGateway replica, String method, String path, String body) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                start.await();
                return send(replica, method, path, body);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private HttpResponse<String> send(KafkaGateway replica, String method, String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + replica.adminBoundPort() + path))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static List<String> names(HttpResponse<String> response, String field) {
        var values = new ArrayList<String>();
        for (JsonNode node : JSON.readTree(response.body())) {
            values.add(node.get(field).asString());
        }
        return values;
    }
}
