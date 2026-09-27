package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import org.junit.jupiter.api.AfterEach;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/// Shared setup for the admin HTTP slice tests: boots a real [AdminHttpServer] on an ephemeral
/// port wired to in-memory fakes, and sends real HTTP requests through the full Netty pipeline.
/// The slice tests assert the JSON wire format the admin UI consumes, so they exercise the
/// server bootstrap, routing, serialization and CORS handling - not just the plain handlers.
abstract class AdminHttpSliceTestBase {

    protected VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of());
    protected MetadataCache cache = new MetadataCache();
    protected FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    protected GovernancePolicy governance = new GovernancePolicy(new GovernanceConfig(null, null));
    protected FakeTopicAdmin topicAdmin = new FakeTopicAdmin();
    protected CorsConfig cors;

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private AdminHttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    protected AdminHttpServer server() {
        return server;
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    /// Boots the server with the current fakes. Call after configuring the fields.
    protected void startServer() throws InterruptedException {
        server = new AdminHttpServer(
                new AdminConfig(true, "127.0.0.1", 0, cors),
                virtualTopics, cache, repository, governance, topicAdmin);
        server.start();
    }

    /// Sends a request to the running server and returns the response. A `null` body sends a
    /// request without a body.
    protected HttpResponse<String> send(String method, String path, String body) throws Exception {
        return send(method, path, body, Map.of());
    }

    /// Sends a request with extra headers to the running server and returns the response.
    protected HttpResponse<String> send(
            String method,
            String path,
            String body,
            Map<String, String> headers
    ) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + server.boundPort() + path));
        headers.forEach(builder::header);
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /// A metadata cache holding the given topics, mirroring a broker snapshot.
    protected static MetadataCache cacheWith(TopicMetadata... topics) {
        var cache = new MetadataCache();
        Map<String, TopicMetadata> topicMap = new HashMap<>();
        for (TopicMetadata topic : topics) {
            topicMap.put(topic.name(), topic);
        }
        cache.update(MetadataSnapshot.of(
                topicMap,
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        return cache;
    }

    /// A topic with `partitions` partitions, each replicated `replicas` times.
    protected static TopicMetadata topic(String name, int partitions, int replicas) {
        var partitionList = new ArrayList<PartitionMetadata>();
        for (int i = 0; i < partitions; i++) {
            partitionList.add(PartitionMetadata.of(
                    i, 1, Collections.nCopies(replicas, 1), List.of(1), List.of()));
        }
        return TopicMetadata.of(name, partitionList);
    }

    /// Asserts the body's top-level property names are exactly `keys` — for a single object, or
    /// for every element of a JSON array. Pins the key set on its own, independently of the values,
    /// so a shape can be asserted without also pinning values that carry no contract, and names the
    /// offending key set in the failure rather than showing a whole-body diff. A partial assertion
    /// such as a single `inPath` field says nothing about the properties beside it. Only the top
    /// level is inspected, so a nested object's keys need an assertion of their own.
    protected static void assertWireKeys(String body, String... keys) {
        var expected = Set.of(keys);
        JsonNode root = JSON.readTree(body);
        var elements = new ArrayList<JsonNode>();
        if (root.isArray()) {
            root.forEach(elements::add);
        } else {
            elements.add(root);
        }
        assertThat(elements).as("response body elements: %s", body).isNotEmpty();
        for (var element : elements) {
            var actual = new TreeSet<String>();
            for (var property : element.properties()) {
                actual.add(property.getKey());
            }
            assertThat(actual)
                    .withFailMessage(() -> "wire keys " + actual + " != expected " + expected + " in: " + body)
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }
}
