package io.jonasg.kawa.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ErrorHeaderConfigTest {

    private static final String FILTERS = """
            {"type": "headerEquals", "header": "h", "value": "v"}
            {"type": "headerContains", "header": "h", "value": "v"}
            {"type": "headerStartsWith", "header": "h", "value": "v"}
            {"type": "headerMatches", "header": "h", "value": "v.*"}
            {"type": "cel", "expression": "true"}
            """;

    private final JsonMapper mapper = JsonMapper.builder().build();

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"type\": \"headerEquals\", \"header\": \"h\", \"value\": \"v\"",
            "{\"type\": \"headerContains\", \"header\": \"h\", \"value\": \"v\"",
            "{\"type\": \"headerStartsWith\", \"header\": \"h\", \"value\": \"v\"",
            "{\"type\": \"headerMatches\", \"header\": \"h\", \"value\": \"v.*\"",
            "{\"type\": \"cel\", \"expression\": \"true\""
    })
    void addsErrorHeadersForAFilterByDefault(String filter) {
        // when
        var config = mapper.readValue(filter + "}", VirtualTopicFilterConfig.class);

        // then
        assertThat(config.errorHeader())
                .withFailMessage(() -> "A filter without 'errorHeader' did not default to adding error headers: " + config)
                .isTrue();
        assertThat(mapper.writeValueAsString(config))
                .withFailMessage(() -> "The default 'errorHeader' was written to the config topic")
                .doesNotContain("errorHeader");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"type\": \"headerEquals\", \"header\": \"h\", \"value\": \"v\"",
            "{\"type\": \"headerContains\", \"header\": \"h\", \"value\": \"v\"",
            "{\"type\": \"headerStartsWith\", \"header\": \"h\", \"value\": \"v\"",
            "{\"type\": \"headerMatches\", \"header\": \"h\", \"value\": \"v.*\"",
            "{\"type\": \"cel\", \"expression\": \"true\""
    })
    void turnsErrorHeadersOffForAFilter(String filter) {
        // when
        var config = mapper.readValue(filter + ", \"errorHeader\": false}", VirtualTopicFilterConfig.class);

        // then
        assertThat(config.errorHeader())
                .withFailMessage(() -> "'errorHeader: false' did not turn error headers off: " + config)
                .isFalse();
        assertThat(mapper.readValue(mapper.writeValueAsString(config), VirtualTopicFilterConfig.class))
                .withFailMessage(() -> "'errorHeader: false' did not survive the config-topic round trip: " + config)
                .isEqualTo(config);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"false\"", "0", "1", "\"no\"", "{}"})
    void refusesAFilterErrorHeaderThatIsNotABoolean(String value) {
        for (String filter : FILTERS.strip().split("\n")) {
            String json = filter.substring(0, filter.length() - 1) + ", \"errorHeader\": " + value + "}";
            assertThatThrownBy(() -> mapper.readValue(json, VirtualTopicFilterConfig.class))
                    .as("A filter with 'errorHeader: %s' was not refused: %s", value, json)
                    .isNotNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"type\": \"dropFields\", \"fields\": [\"a\"]",
            "{\"type\": \"renameField\", \"from\": \"a\", \"to\": \"b\"",
            "{\"type\": \"defaultField\", \"field\": \"a\", \"value\": 1",
            "{\"type\": \"cel\", \"expression\": \"value\"",
            "{\"type\": \"convert\", \"format\": {\"type\": \"json\"}",
            "{\"type\": \"dropHeaders\", \"headers\": [\"h\"]",
            "{\"type\": \"setHeader\", \"header\": \"h\", \"value\": \"v\""
    })
    void addsErrorHeadersForAStepByDefault(String step) {
        // when
        var config = mapper.readValue(step + "}", TransformationConfig.class);

        // then
        assertThat(config.errorHeader())
                .withFailMessage(() -> "A step without 'errorHeader' did not default to adding error headers: " + config)
                .isTrue();
        assertThat(mapper.writeValueAsString(config))
                .withFailMessage(() -> "The default 'errorHeader' was written to the config topic")
                .doesNotContain("errorHeader");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"type\": \"dropFields\", \"fields\": [\"a\"]",
            "{\"type\": \"renameField\", \"from\": \"a\", \"to\": \"b\"",
            "{\"type\": \"defaultField\", \"field\": \"a\", \"value\": 1",
            "{\"type\": \"cel\", \"expression\": \"value\"",
            "{\"type\": \"convert\", \"format\": {\"type\": \"json\"}",
            "{\"type\": \"dropHeaders\", \"headers\": [\"h\"]",
            "{\"type\": \"setHeader\", \"header\": \"h\", \"value\": \"v\""
    })
    void turnsErrorHeadersOffForAStep(String step) {
        // when
        var config = mapper.readValue(step + ", \"errorHeader\": false}", TransformationConfig.class);

        // then
        assertThat(config.errorHeader())
                .withFailMessage(() -> "'errorHeader: false' did not turn error headers off: " + config)
                .isFalse();
        assertThat(mapper.readValue(mapper.writeValueAsString(config), TransformationConfig.class))
                .withFailMessage(() -> "'errorHeader: false' did not survive the config-topic round trip: " + config)
                .isEqualTo(config);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"type\": \"dropFields\", \"fields\": [\"a\"]",
            "{\"type\": \"renameField\", \"from\": \"a\", \"to\": \"b\"",
            "{\"type\": \"defaultField\", \"field\": \"a\", \"value\": 1",
            "{\"type\": \"cel\", \"expression\": \"value\"",
            "{\"type\": \"convert\", \"format\": {\"type\": \"json\"}",
            "{\"type\": \"dropHeaders\", \"headers\": [\"h\"]",
            "{\"type\": \"setHeader\", \"header\": \"h\", \"value\": \"v\""
    })
    void refusesAStepErrorHeaderThatIsNotABoolean(String step) {
        for (String value : new String[]{"\"false\"", "0", "1", "\"no\"", "{}"}) {
            String json = step + ", \"errorHeader\": " + value + "}";
            assertThatThrownBy(() -> mapper.readValue(json, TransformationConfig.class))
                    .as("A step with 'errorHeader: %s' was not refused: %s", value, json)
                    .isNotNull();
        }
    }
}
