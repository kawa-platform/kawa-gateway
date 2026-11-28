package io.jonasg.kawa.it;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.BootstrapConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.http.users.AdminSetup;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
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
import java.util.regex.Pattern;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// Two replicas in setup mode (`oauth`, no bootstrap user, an empty config topic), each with the setup
/// token it generated and logged. The first admin user is created as a compare-and-set on the shared
/// config topic, so of concurrent setups on both replicas exactly one succeeds.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AdminSetupReplicasIT {

    private static final String CONFIG_TOPIC = "__kawa_setup";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Pattern LOGGED_TOKEN = Pattern.compile("setup token:\\s+([A-Za-z0-9_-]+)");

    @Container
    static KafkaContainer kafka = new KafkaContainer(KafkaImage.KAFKA_NATIVE);

    private final HttpClient http = HttpClient.newHttpClient();
    private final List<KafkaGateway> replicas = new ArrayList<>();
    private final List<String> tokens = new ArrayList<>();

    @BeforeAll
    void setUp() throws Exception {
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }
        startReplica();
        startReplica();
    }

    @AfterAll
    void tearDown() {
        replicas.forEach(KafkaGateway::stop);
    }

    @Test
    void concurrentSetupsOnTwoReplicasCreateExactlyOneAdminUser() throws Exception {
        // given
        var start = new CountDownLatch(1);
        var attempts = new ArrayList<CompletableFuture<HttpResponse<String>>>();
        for (int i = 0; i < 5; i++) {
            for (int replica = 0; replica < replicas.size(); replica++) {
                attempts.add(setupAsync(start, replica, "admin-" + replica + "-" + i + "@mycom.com"));
            }
        }

        // when
        start.countDown();

        // then
        List<Integer> statuses = attempts.stream().map(attempt -> attempt.join().statusCode()).toList();
        assertThat(statuses)
                .withFailMessage(() -> "not exactly one concurrent setup succeeded: " + statuses)
                .containsOnlyOnce(201)
                .containsOnly(201, 409);
        for (int replica = 0; replica < replicas.size(); replica++) {
            int index = replica;
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(JSON.readTree(send(index, "GET", "/admin/setup", null).body()).get("required").asBoolean())
                            .withFailMessage(() -> "replica " + index + " still requires setup")
                            .isFalse());
        }
    }

    private void startReplica() throws Exception {
        var setupLog = (Logger) LoggerFactory.getLogger(AdminSetup.class);
        var logged = new ListAppender<ILoggingEvent>();
        logged.start();
        setupLog.addAppender(logged);
        try {
            var bootstrap = new BootstrapConfig(
                    List.of(new ListenerConfig("127.0.0.1", 0)),
                    Map.of("default", new ClusterConfig(List.of(kafka.getBootstrapServers()))),
                    new AdvertisedListener(1, "localhost", 0),
                    null,
                    new AdminConfig(true, "127.0.0.1", 0, null, new AdminAuthConfig(null, null, Map.of(
                            "loginUrl", "http://localhost:5173/admin/login",
                            "redirectUris", "http://localhost:5173/admin/auth/callback"))),
                    CONFIG_TOPIC);
            var gateway = new KafkaGateway(bootstrap);
            gateway.start();
            replicas.add(gateway);
        } finally {
            setupLog.detachAppender(logged);
        }
        var token = logged.list.stream()
                .map(event -> LOGGED_TOKEN.matcher(event.getFormattedMessage()))
                .filter(java.util.regex.Matcher::find)
                .map(matcher -> matcher.group(1))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the replica did not log a setup token: " + logged.list));
        tokens.add(token);
    }

    private CompletableFuture<HttpResponse<String>> setupAsync(CountDownLatch start, int replica, String email) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                start.await();
                return send(replica, "POST", "/admin/setup", JSON.writeValueAsString(Map.of(
                        "token", tokens.get(replica), "email", email, "password", "secret")));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private HttpResponse<String> send(int replica, String method, String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + replicas.get(replica).adminBoundPort() + path))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
