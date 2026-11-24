package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

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
    void acceptsHeaderStepsWithoutAValueFormat() throws Exception {
        // given a topic that only rewrites headers
        String json = """
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "transformations": [{"type": "dropHeaders", "headers": ["__TypeId__"]}]
                }}}
                """;

        // when
        var virtualTopic = mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.virtual");

        // then
        assertThat(virtualTopic.transformations()).containsExactly(new DropHeadersTransformationConfig(List.of("__TypeId__")));
        assertThat(virtualTopic.outputFormat())
                .withFailMessage(() -> "A header-only topic got an output format: " + virtualTopic.outputFormat())
                .isNull();
    }

    @Test
    void refusesAConvertWithoutAValueFormat() {
        assertThatThrownBy(() -> new VirtualTopicConfig("orders.raw", null, false, null,
                List.of(new ConvertTransformationConfig(new JsonFormatConfig())), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("require a 'valueFormat'");
    }

    @Test
    void refusesAValueStepAfterConvertNamingTheConvert() {
        // given
        var steps = List.<TransformationConfig>of(
                new DropFieldsTransformationConfig(List.of("internal")),
                new ConvertTransformationConfig(new JsonFormatConfig()),
                new DropFieldsTransformationConfig(List.of("secret")));

        // when / then
        assertThatThrownBy(() -> new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(), steps, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("transformations[1] (convert): only header steps may follow it");
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
    void roundTripsAnXmlOutputThroughTheConfigTopic() throws Exception {
        // given
        var virtualTopic = new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(),
                List.of(new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order"))),
                null);
        var config = DynamicConfig.empty().upsertVirtualTopic("orders.xml", virtualTopic);

        // when
        String json = mapper.writeValueAsString(config);

        // then
        assertThat(mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.xml"))
                .withFailMessage(() -> "The XML format did not survive the config-topic round trip: " + json)
                .isEqualTo(virtualTopic);
    }

    @Test
    void refusesAnXmlOutputWithoutARoot() {
        var steps = List.<TransformationConfig>of(new DropFieldsTransformationConfig(List.of("secret")));

        assertThatThrownBy(() -> new VirtualTopicConfig("orders.raw", null, false, new XmlFormatConfig(), steps, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("XML requires a 'root'");
    }

    @Test
    void readsADefaultFieldWithAnyJsonValue() throws Exception {
        // when
        var virtualTopic = mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "valueFormat": {"type": "json"},
                  "transformations": [
                    {"type": "defaultField", "field": "currency", "value": "EUR"},
                    {"type": "defaultField", "field": "shipping.address", "value": {"country": "BE", "lines": [1, 2]}}
                  ]
                }}}
                """, DynamicConfig.class).virtualTopics().get("orders.virtual");

        // then
        assertThat(virtualTopic.transformations())
                .withFailMessage(() -> "The defaultField steps were not read: " + virtualTopic.transformations())
                .containsExactly(
                        new DefaultFieldTransformationConfig("currency", "EUR"),
                        new DefaultFieldTransformationConfig("shipping.address", Map.of("country", "BE", "lines", List.of(1, 2))));
    }

    @Test
    void refusesInvalidFieldPaths() {
        for (String path : List.of("", "a..b", ".a", "a.")) {
            assertThatThrownBy(() -> new DropFieldsTransformationConfig(List.of(path)))
                    .withFailMessage(() -> "The field path '" + path + "' was accepted")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void refusesARenameIntoItsOwnPath() {
        assertThatThrownBy(() -> new RenameFieldTransformationConfig("customer", "customer.name"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one contains the other");
        assertThatThrownBy(() -> new RenameFieldTransformationConfig("id", "id"))
                .isInstanceOf(IllegalArgumentException.class);
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

    @Test
    void roundTripsHeaderStepsAfterConvertThroughTheConfigTopic() throws Exception {
        // given
        var virtualTopic = new VirtualTopicConfig("orders.raw", null, false, new AvroFormatConfig(),
                List.of(new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                        new ConvertTransformationConfig(new JsonFormatConfig()),
                        new DropHeadersTransformationConfig(List.of("__TypeId__", "content-type")),
                        new SetHeaderTransformationConfig("content-type", "application/json")),
                null);
        var config = DynamicConfig.empty().upsertVirtualTopic("orders.virtual", virtualTopic);

        // when
        String json = mapper.writeValueAsString(config);

        // then
        var read = mapper.readValue(json, DynamicConfig.class).virtualTopics().get("orders.virtual");
        assertThat(read)
                .withFailMessage(() -> "The header steps did not survive the config-topic round trip: " + json)
                .isEqualTo(virtualTopic);
        assertThat(read.outputFormat())
                .withFailMessage(() -> "The convert format followed by header steps was not the output format: " + read.outputFormat())
                .isEqualTo(new JsonFormatConfig());
    }

    @Test
    void readsHeaderStepsBeforeConvert() {
        // when
        var virtualTopic = new VirtualTopicConfig("orders.raw", null, false, new AvroFormatConfig(),
                List.of(new SetHeaderTransformationConfig("content-type", "application/json"),
                        new ConvertTransformationConfig(new JsonFormatConfig())),
                null);

        // then
        assertThat(virtualTopic.outputFormat())
                .withFailMessage(() -> "The convert format was not the output format: " + virtualTopic.outputFormat())
                .isEqualTo(new JsonFormatConfig());
    }

    @Test
    void refusesASecondConvertNamingTheFirst() {
        // given
        var steps = List.<TransformationConfig>of(
                new ConvertTransformationConfig(new JsonFormatConfig()),
                new DropHeadersTransformationConfig(List.of("content-type")),
                new ConvertTransformationConfig(new JsonFormatConfig()));

        // when / then
        assertThatThrownBy(() -> new VirtualTopicConfig("orders.raw", null, false, new JsonFormatConfig(), steps, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("transformations[0] (convert): only header steps may follow it");
    }

    @Test
    void refusesInvalidHeaderSteps() {
        assertThatThrownBy(() -> new DropHeadersTransformationConfig(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dropHeaders transformation requires at least one header");
        assertThatThrownBy(() -> new DropHeadersTransformationConfig(List.of("content-type", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dropHeaders transformation: header names must not be blank");
        assertThatThrownBy(() -> new SetHeaderTransformationConfig(" ", "application/json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("setHeader transformation requires a 'header'");
        assertThatThrownBy(() -> new SetHeaderTransformationConfig("content-type", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("setHeader transformation requires a 'value'");
    }

    @Test
    void refusesAnUnknownOutputFormat() {
        assertThatThrownBy(() -> mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "valueFormat": {"type": "json"},
                  "transformations": [{"type": "convert", "format": {"type": "protobuf"}}]
                }}}
                """, DynamicConfig.class))
                .hasMessageContaining("Invalid transformation for virtual topic 'orders.virtual'");
    }

    @Test
    void refusesAnUnknownValueFormat() {
        assertThatThrownBy(() -> mapper.readValue("""
                {"virtualTopics": {"orders.virtual": {
                  "topic": "orders.raw",
                  "valueFormat": {"type": "protobuf"}
                }}}
                """, DynamicConfig.class))
                .hasMessageContaining("protobuf");
    }
}
