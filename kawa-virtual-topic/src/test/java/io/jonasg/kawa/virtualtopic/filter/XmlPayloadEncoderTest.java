package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class XmlPayloadEncoderTest {

    private final XmlPayloadDecoder decoder = new XmlPayloadDecoder(null);

    @ParameterizedTest
    @ValueSource(strings = {
        XmlPayloadDecoderTest.ORDER,
        "<r><line><sku>A</sku><qty>1</qty></line><line><sku>B</sku><qty>2</qty></line><line><sku>C</sku></line></r>",
        "<soap:Envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns=\"urn:default\">"
                + "<soap:Body><m:price xmlns:m=\"urn:m\" m:unit=\"EUR\">9.99</m:price></soap:Body></soap:Envelope>",
        "<r><name lang=\"fr\">Zoë Ångström</name><city>東京</city><mood>🚀</mood></r>",
        "<r a=\"&lt;&amp;&quot;'\"><t>&lt;tag&gt; &amp; \"quotes\" 'apostrophes'</t></r>",
        "<r><empty/><blank></blank><spaces>   </spaces></r>",
        "<r><deep><er><est>x</est></er></deep></r>",
        "<greeting>hello</greeting>"
    })
    void decodingAnEncodedValueGivesTheSameValueBack(String xml) {
        // given
        Object value = XmlPayloadDecoderTest.decode(decoder, xml);
        String root = xml.substring(xml.indexOf('<') + 1).split("[ >/]", 2)[0];

        // when
        byte[] encoded = new XmlPayloadEncoder(root).encode(value);

        // then
        assertThat(decoder.decode(ByteBuffer.wrap(encoded)))
                .withFailMessage(() -> "The value of " + xml + " did not survive the round trip, encoded as "
                        + new String(encoded, StandardCharsets.UTF_8))
                .isEqualTo(value);
    }

    @Test
    void writesTheDocumentedXml() {
        // given
        Object value = XmlPayloadDecoderTest.decode(decoder, XmlPayloadDecoderTest.ORDER);

        // when
        String xml = encode("order", value);

        // then
        assertThat(xml)
                .withFailMessage(() -> "The order was not written per the documented mapping: " + xml)
                .isEqualTo("<order id=\"7\" xmlns:x=\"urn:x\"><x:total currency=\"EUR\">12.50</x:total>"
                        + "<line>A</line><line>B</line><note></note></order>");
    }

    @Test
    void movesAnAttributeToAnElementWhenRenamed() {
        // given a renameField from @id to id
        @SuppressWarnings("unchecked")
        Map<String, Object> value = new LinkedHashMap<>(
                (Map<String, Object>) XmlPayloadDecoderTest.decode(decoder, "<order id=\"7\"><status>PAID</status></order>"));
        value.put("id", value.remove("@id"));

        // when
        String xml = encode("order", value);

        // then
        assertThat(xml)
                .withFailMessage(() -> "The renamed attribute was not written as an element: " + xml)
                .isEqualTo("<order><status>PAID</status><id>7</id></order>");
    }

    @Test
    void writesJsonScalarsAsText() {
        // given
        Object json = new JsonPayloadDecoder().decode(ByteBuffer.wrap(
                "{\"id\":42,\"total\":12.5,\"paid\":true,\"note\":null,\"tags\":[\"a\",\"b\"]}".getBytes(StandardCharsets.UTF_8)));

        // when
        String xml = encode("order", json);

        // then
        assertThat(xml)
                .withFailMessage(() -> "The JSON document was not written as XML text: " + xml)
                .isEqualTo("<order><id>42</id><total>12.5</total><paid>true</paid><note/><tags>a</tags><tags>b</tags></order>");
    }

    @Test
    void writesAvroAsXml() {
        // given
        Object avro = new AvroPayloadDecoder(id -> AvroPayloadEncoderTest.ORDER).decode(
                ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, AvroPayloadEncoderTest.ORDER, AvroPayloadEncoderTest.order())));

        // when
        String xml = encode("order", avro);

        // then
        assertThat(xml)
                .withFailMessage(() -> "The Avro order was not written as XML: " + xml)
                .isEqualTo("<order><id>42</id><qty>3</qty><status>PAID</status><customer><name>Zoë 東京 🚀</name></customer>"
                        + "<lines><sku>A</sku><price>9.99</price></lines><tags><channel>web</channel></tags><note/>"
                        + "<weight>1.5</weight><paid>true</paid><total>12.34</total><signature>AQID</signature>"
                        + "<checksum>3q2+7w==</checksum><createdAt>1700000000000</createdAt></order>");
    }

    @Test
    void refusesValuesXmlCannotHold() {
        Map<String, Object> invalidName = Map.of("my field", "x");
        Map<String, Object> objectAttribute = Map.of("@a", Map.of("b", "c"));
        Map<String, Object> listInList = Map.of("a", List.of(List.of("x")));
        Map<String, Object> mixed = new LinkedHashMap<>(Map.of("#text", "t", "a", "1"));
        Map<String, Object> controlChar = Map.of("a", "bell\u0007");
        Map<String, Object> nullAttribute = Map.of("@a", NullValue.NULL_VALUE);

        assertThatThrownBy(() -> new XmlPayloadEncoder("r").encode(controlChar))
                .withFailMessage(() -> "The failure quoted the character XML cannot carry")
                .hasMessageNotContaining("0007");
        for (Object invalid : List.of(invalidName, objectAttribute, listInList, mixed, controlChar, nullAttribute, List.of("a"))) {
            assertThatThrownBy(() -> new XmlPayloadEncoder("r").encode(invalid))
                    .withFailMessage(() -> "A value XML cannot hold was encoded: " + invalid)
                    .isInstanceOf(PayloadEncodeException.class);
        }
    }

    @Test
    void writesBytesAsBase64() {
        assertThat(encode("r", Map.of("b", CelByteString.of(new byte[] {1, 2, 3})))).isEqualTo("<r><b>AQID</b></r>");
    }

    @Test
    void refusesAnInvalidRootName() {
        assertThatThrownBy(() -> new XmlPayloadEncoder("1st"))
                .isInstanceOf(PayloadEncodeException.class)
                .hasMessageContaining("not a valid XML name");
    }

    private static String encode(String root, Object value) {
        return new String(new XmlPayloadEncoder(root).encode(value), StandardCharsets.UTF_8);
    }
}
