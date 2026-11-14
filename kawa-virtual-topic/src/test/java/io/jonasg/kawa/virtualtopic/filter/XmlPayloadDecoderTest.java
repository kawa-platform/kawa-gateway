package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.NullValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class XmlPayloadDecoderTest {

    static final String ORDER = """
            <order id="7" xmlns:x="urn:x">
              <x:total currency="EUR">12.50</x:total>
              <line>A</line><line>B</line>
              <note/>
            </order>
            """;

    private final XmlPayloadDecoder decoder = new XmlPayloadDecoder(null);

    @Test
    void decodesTheRootContentIntoAValueTree() {
        // when
        Object value = decode(decoder, ORDER);

        // then
        assertThat(value)
                .withFailMessage(() -> "The order was not decoded per the documented mapping: " + value)
                .isEqualTo(Map.of(
                        "@id", "7",
                        "@xmlns:x", "urn:x",
                        "x:total", Map.of("@currency", "EUR", "#text", "12.50"),
                        "line", List.of("A", "B"),
                        "note", ""));
    }

    @Test
    void keepsTextAsStringsVerbatim() {
        // when
        Object value = decode(decoder, "<r><zip>007</zip><qty>3</qty><padded>  a  </padded></r>");

        // then
        assertThat(value)
                .withFailMessage(() -> "Text was converted or trimmed: " + value)
                .isEqualTo(Map.of("zip", "007", "qty", "3", "padded", "  a  "));
    }

    @Test
    void readsCdataAndEscapesAsText() {
        // when
        Object value = decode(decoder, "<r><a><![CDATA[x < y & z]]></a><b>&lt;&amp;&quot;</b></r>");

        // then
        assertThat(value)
                .withFailMessage(() -> "CDATA or escaped text was not read as plain text: " + value)
                .isEqualTo(Map.of("a", "x < y & z", "b", "<&\""));
    }

    @Test
    void readsATextOnlyRootAsAString() {
        assertThat(decode(decoder, "<greeting>hello</greeting>")).isEqualTo("hello");
    }

    @Test
    void readsAnAbsentFieldAsNull() {
        // given
        @SuppressWarnings("unchecked")
        var value = (Map<String, Object>) decode(decoder, ORDER);

        // then
        assertThat(value.get("status"))
                .withFailMessage(() -> "An absent field did not read as null")
                .isEqualTo(NullValue.NULL_VALUE);
    }

    @Test
    void dropsCommentsAndProcessingInstructions() {
        // when
        Object value = decode(decoder, "<?xml version=\"1.0\"?><!-- c --><r><?pi x?><a>1</a><!-- c --></r>");

        // then
        assertThat(value)
                .withFailMessage(() -> "Comments or processing instructions ended up in the value: " + value)
                .isEqualTo(Map.of("a", "1"));
    }

    @Test
    void refusesAnotherRootThanTheConfiguredOne() {
        assertThatThrownBy(() -> decode(new XmlPayloadDecoder("invoice"), ORDER))
                .isInstanceOf(PayloadDecodeException.class)
                .hasMessageContaining("root element <order>, expected <invoice>");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "<r>text<a>1</a></r>",
        "<r><a>1</a><b>2</b><a>3</a></r>",
        "<!DOCTYPE r [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><r>&xxe;</r>",
        "<!DOCTYPE r [<!ENTITY a \"aaaa\"><!ENTITY b \"&a;&a;&a;&a;\">]><r>&b;</r>",
        "<r><a>1</r>",
        "not xml",
        "<r>&undeclared;</r>",
        "   "
    })
    void refusesWhatTheValueTreeCannotHoldOrIsUnsafe(String xml) {
        assertThatThrownBy(() -> decode(decoder, xml))
                .withFailMessage(() -> "Decoding was not refused for: " + xml)
                .isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    void namesTheRejectedConstruct() {
        assertThatThrownBy(() -> decode(decoder, "<r>text<a>1</a></r>")).hasMessageContaining("mixed content");
        assertThatThrownBy(() -> decode(decoder, "<r><a>1</a><b>2</b><a>3</a></r>")).hasMessageContaining("repeats <a>");
        assertThatThrownBy(() -> decode(decoder, "<!DOCTYPE r><r/>")).hasMessageContaining("DOCTYPE");
    }

    @Test
    void refusesAnEmptyPayload() {
        assertThatThrownBy(() -> decoder.decode(ByteBuffer.allocate(0)))
                .isInstanceOf(PayloadDecodeException.class)
                .hasMessageContaining("empty");
    }

    static Object decode(XmlPayloadDecoder decoder, String xml) {
        return decoder.decode(ByteBuffer.wrap(xml.getBytes(StandardCharsets.UTF_8)));
    }
}
