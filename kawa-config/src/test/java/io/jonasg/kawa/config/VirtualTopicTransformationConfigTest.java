package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualTopicTransformationConfigTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void readsTransformationsFromTheConfigTopic() throws Exception {
        // given
        String json = """
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "valueFormat": {"type": "avro"},
                  "transformations": [
                    {"type": "dropFields", "fields": ["internal_risk_score"]},
                    {"type": "renameField", "from": "order_id", "to": "id"},
                    {"type": "cel", "expression": "{'id': value.id, 'total': value.total}"},
                    {"type": "convert", "format": {"type": "json"}}
                  ]
                }}}
                """;

        // when
        var virtualTopic = mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.virtual");

        // then
        assertThat(virtualTopic.transformations())
                .withFailMessage(() -> "The transformations were not read in order: " + virtualTopic.transformations())
                .containsExactly(
                        new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                        new RenameFieldTransformationConfig("order_id", "id"),
                        new CelTransformationConfig("{'id': value.id, 'total': value.total}"),
                        new ConvertTransformationConfig(new JsonFormatConfig()));
    }

    @Test
    void roundTripsTransformationsThroughTheConfigTopic() throws Exception {
        // given
        var virtualTopic = new VirtualTopicConfig("orders.raw", null, false, new AvroFormatConfig(),
                List.of(new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                        new ConvertTransformationConfig(new JsonFormatConfig())),
                null);
        var config = DynamicConfig.empty().upsertVirtualTopic("orders.virtual", virtualTopic);

        // when
        String json = mapper.writeValueAsString(config);

        // then
        assertThat(mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.virtual"))
                .withFailMessage(() -> "The transformations did not survive the config-topic round trip: " + json)
                .isEqualTo(virtualTopic);
    }

    @Test
    void passesTheValueThroughWhenTransformationsAreAbsent() throws Exception {
        // when
        var virtualTopic = mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {"topic": "orders.raw"}}}
                """, DynamicConfig.class).virtualTopics().get("orders.virtual");

        // then
        assertThat(virtualTopic.transformations())
                .withFailMessage(() -> "Absent transformations did not read as none: " + virtualTopic.transformations())
                .isEmpty();
    }

    @Test
    void refusesTransformationsWithoutAValueFormat() {
        assertThatThrownBy(() -> mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "transformations": [{"type": "dropFields", "fields": ["secret"]}]
                }}}
                """, DynamicConfig.class))
                .hasMessageContaining("orders.virtual")
                .hasMessageContaining("require a 'valueFormat'");
    }

    @Test
    void refusesAConvertThatIsNotTheLastStep() {
        var steps = List.<TransformationConfig>of(
                new ConvertTransformationConfig(new JsonFormatConfig()),
                new DropFieldsTransformationConfig(List.of("secret")));

        assertThatThrownBy(() -> new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(), steps, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be the last step");
    }

    @Test
    void roundTripsAnAvroOutputSubjectThroughTheConfigTopic() throws Exception {
        // given
        var virtualTopic = new VirtualTopicConfig("orders.raw", null, false, new AvroFormatConfig(),
                List.of(new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                        new ConvertTransformationConfig(new AvroFormatConfig(DecodeErrorPolicy.SKIP, "orders.virtual-value", 3))),
                null);
        var config = DynamicConfig.empty().upsertVirtualTopic("orders.virtual", virtualTopic);

        // when
        String json = mapper.writeValueAsString(config);

        // then
        assertThat(mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.virtual"))
                .withFailMessage(() -> "The Avro subject and version did not survive the config-topic round trip: " + json)
                .isEqualTo(virtualTopic);
    }

    @Test
    void readsAnAvroSubjectWithoutAVersionAsTheLatest() throws Exception {
        // when
        var virtualTopic = mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "valueFormat": {"type": "avro", "subject": "orders.virtual-value"},
                  "transformations": [{"type": "dropFields", "fields": ["internal_risk_score"]}]
                }}}
                """, DynamicConfig.class).virtualTopics().get("orders.virtual");

        // then
        assertThat(virtualTopic.valueFormat())
                .withFailMessage(() -> "The Avro subject was not read as the latest version: " + virtualTopic.valueFormat())
                .isEqualTo(new AvroFormatConfig(DecodeErrorPolicy.SKIP, "orders.virtual-value", null));
    }

    @Test
    void refusesAnAvroOutputWithoutASubject() {
        var steps = List.<TransformationConfig>of(new DropFieldsTransformationConfig(List.of("secret")));

        assertThatThrownBy(() -> new VirtualTopicConfig("orders.raw", null, false, new AvroFormatConfig(), steps, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a 'subject'");
    }

    @Test
    void refusesAnAvroVersionWithoutASubject() {
        assertThatThrownBy(() -> new AvroFormatConfig(DecodeErrorPolicy.SKIP, null, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'version' requires a 'subject'");
    }

    @Test
    void refusesAnAvroVersionBelowOne() {
        assertThatThrownBy(() -> new AvroFormatConfig(DecodeErrorPolicy.SKIP, "orders-value", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1 or higher");
    }

    @Test
    void refusesAnUnknownTransformationType() {
        assertThatThrownBy(() -> mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "valueFormat": {"type": "json"},
                  "transformations": [{"type": "uppercase"}]
                }}}
                """, DynamicConfig.class))
                .hasMessageContaining("Invalid transformation for virtual topic 'orders.virtual'");
    }
}
