package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionalConfigTopicWriterTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void serializeRoundTripsThroughConsumerDeserialization() throws Exception {
        // given
        try (var writer = new TransactionalConfigTopicWriter("localhost:9092", "__kawa", new Properties())) {
            var config = new DynamicConfig(
                    Map.of("orders", new VirtualTopicConfig("raw-orders")),
                    new AuthConfig(Set.of("PLAIN"),
                            Map.of("alice", new ClientConfig("PLAIN",
                                    HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret")))),
                    new RbacConfig(Map.of("reader", new RoleConfig(List.of())), null), null);

            // when
            String json = writer.serialize(config);
            DynamicConfig readBack = mapper.readValue(json, DynamicConfig.class);

            // then
            assertThat(readBack).isEqualTo(config);
        }
    }

    @Test
    void serializeRoundTripsVirtualTopicValueFormat() throws Exception {
        // given a virtual topic filtering on JSON content
        try (var writer = new TransactionalConfigTopicWriter("localhost:9092", "__kawa", new Properties())) {
            var virtualTopic = new VirtualTopicConfig(
                    "orders",
                    new CelFilterConfig("value.status == \"PAID\""),
                    false,
                    new JsonFormatConfig(DecodeErrorPolicy.INCLUDE));
            var config = DynamicConfig.empty().upsertVirtualTopic("paid-orders", virtualTopic);

            // when
            String json = writer.serialize(config);
            DynamicConfig readBack = mapper.readValue(json, DynamicConfig.class);

            // then
            assertThat(readBack.virtualTopics().get("paid-orders"))
                    .withFailMessage(() -> "Virtual topic valueFormat did not survive the config-topic round trip: " + json)
                    .isEqualTo(virtualTopic);
        }
    }

    @Test
    void defaultsOnDecodeErrorToSkipWhenOmitted() throws Exception {
        // given a snapshot written without onDecodeError
        String json = """
                {"virtualTopics": {"paid-orders": {
                  "topic": "orders",
                  "filter": {"type": "cel", "expression": "value.status == \\"PAID\\""},
                  "valueFormat": {"type": "json"}
                }}}
                """;

        // when
        DynamicConfig config = mapper.readValue(json, DynamicConfig.class);

        // then
        assertThat(config.virtualTopics().get("paid-orders").valueFormat())
                .withFailMessage(() -> "Omitted onDecodeError did not default to skip")
                .isEqualTo(new JsonFormatConfig(DecodeErrorPolicy.SKIP));
    }

    @Test
    void serializeProducesFullSnapshotWithAllDynamicSections() throws Exception {
        // given
        try (var writer = new TransactionalConfigTopicWriter("localhost:9092", "__kawa", new Properties())) {
            var config = new DynamicConfig(
                    Map.of("orders", new VirtualTopicConfig("raw-orders")),
                    new AuthConfig(Set.of("PLAIN"),
                            Map.of("alice", new ClientConfig("PLAIN",
                                    HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret")))),
                    new RbacConfig(Map.of("reader", new RoleConfig(List.of())), null), null);

            // when
            String json = writer.serialize(config);

            // then
            assertThat(json).contains("\"virtualTopics\"", "\"auth\"", "\"rbac\"");
        }
    }
}
