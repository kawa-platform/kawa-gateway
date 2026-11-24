package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderMappingsTest {

    @Test
    void dropsEveryHeaderWithANamedKey() {
        // given
        HeaderMapping mapping = HeaderMappings.compile(List.of(new DropHeadersTransformationConfig(List.of("content-type"))));

        // when
        Header[] headers = mapping.apply(new Header[]{header("content-type", "a"), header("region", "eu"), header("content-type", "b")});

        // then
        assertThat(headers).withFailMessage(() -> "The content-type headers were not all dropped: " + List.of(headers))
                .containsExactly(header("region", "eu"));
    }

    @Test
    void matchesHeaderNamesCaseSensitively() {
        // given
        HeaderMapping mapping = HeaderMappings.compile(List.of(new DropHeadersTransformationConfig(List.of("content-type"))));

        // when
        Header[] headers = mapping.apply(new Header[]{header("Content-Type", "a")});

        // then
        assertThat(headers).withFailMessage(() -> "A header with another case was dropped")
                .containsExactly(header("Content-Type", "a"));
    }

    @Test
    void setsAHeaderReplacingEveryHeaderWithItsName() {
        // given
        HeaderMapping mapping = HeaderMappings.compile(List.of(new SetHeaderTransformationConfig("content-type", "application/json")));

        // when
        Header[] headers = mapping.apply(new Header[]{header("content-type", "a"), header("region", "eu"), header("content-type", "b")});

        // then
        assertThat(headers).withFailMessage(() -> "The content-type headers were not replaced by one: " + List.of(headers))
                .containsExactly(header("region", "eu"), header("content-type", "application/json"));
    }

    @Test
    void appliesHeaderStepsInOrderIgnoringValueSteps() {
        // given a set followed by a drop of the same header
        HeaderMapping mapping = HeaderMappings.compile(List.of(
                new SetHeaderTransformationConfig("content-type", "application/json"),
                new DropFieldsTransformationConfig(List.of("a")),
                new ConvertTransformationConfig(new JsonFormatConfig()),
                new DropHeadersTransformationConfig(List.of("content-type"))));

        // when
        Header[] headers = mapping.apply(new Header[]{header("region", "eu")});

        // then
        assertThat(headers).withFailMessage(() -> "The steps were not applied in order: " + List.of(headers))
                .containsExactly(header("region", "eu"));
    }

    @Test
    void keepsHeadersWithoutHeaderSteps() {
        // when
        HeaderMapping mapping = HeaderMappings.compile(List.of(new DropFieldsTransformationConfig(List.of("a"))));

        // then
        assertThat(mapping).withFailMessage(() -> "Value steps alone changed the headers").isSameAs(HeaderMapping.IDENTITY);
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
