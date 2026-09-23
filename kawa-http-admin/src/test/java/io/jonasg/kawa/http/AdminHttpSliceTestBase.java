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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
