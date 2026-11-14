package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonPayloadEncoderTest {

    private final JsonPayloadDecoder decoder = new JsonPayloadDecoder();
    private final JsonPayloadEncoder encoder = new JsonPayloadEncoder();

    @ParameterizedTest
    @ValueSource(strings = {
        "{\"id\":42,\"customer\":{\"name\":\"Ada\",\"address\":{\"city\":\"Ghent\",\"zip\":\"9000\"}}}",
        "[1,2.5,\"three\",true,false,null,[],{},[[1],[2,[3]]]]",
        "{\"lines\":[{\"sku\":\"A\",\"qty\":1},{\"sku\":\"B\",\"qty\":2}]}",
        "{\"name\":\"Zoë Ångström\",\"city\":\"東京\",\"mood\":\"🚀\",\"escaped\":\"\\u0000 \\\" \\\\ \\n\"}",
        "{\"max\":9223372036854775807,\"min\":-9223372036854775808}",
        "{\"huge\":12345678901234567890,\"precise\":3.141592653589793,\"tiny\":1.0E-300}",
        "{\"integral\":1,\"floating\":1.0,\"exponent\":1e2}",
        "{\"present\":null,\"empty\":\"\",\"zero\":0,\"no\":false}",
        "\"just a string\"",
        "-17",
        "null"
    })
    void decodingAnEncodedValueGivesTheSameValueBack(String json) {
        // given
        Object value = decode(json);

        // when
        byte[] encoded = encoder.encode(value);

        // then
        assertThat(decoder.decode(ByteBuffer.wrap(encoded)))
                .withFailMessage(() -> "The value of " + json + " did not survive the round trip, encoded as "
                        + new String(encoded, StandardCharsets.UTF_8))
                .isEqualTo(value);
    }

    @Test
    void keepsAnExplicitNullDistinctFromAnAbsentField() {
        // when
        String withNull = encode(decode("{\"a\":null}"));
        String withoutField = encode(decode("{}"));

        // then
        assertThat(withNull)
                .withFailMessage(() -> "The explicit null field was not written: " + withNull)
                .isEqualTo("{\"a\":null}");
        assertThat(withoutField)
                .withFailMessage(() -> "An absent field was written: " + withoutField)
                .isEqualTo("{}");
    }

    @Test
    void keepsIntegralAndFloatingPointNumbersApart() {
        // when
        String json = encode(decode("{\"integral\":1,\"floating\":1.0}"));

        // then
        assertThat(json)
                .withFailMessage(() -> "A whole Double lost its fraction or a Long gained one: " + json)
                .isEqualTo("{\"integral\":1,\"floating\":1.0}");
    }

    @Test
    void writesIntegersBeyondALongAsTheDoubleTheyWereDecodedTo() {
        // when
        String json = encode(decode("{\"id\":12345678901234567890}"));

        // then
        assertThat(json)
                .withFailMessage(() -> "An integer beyond long was not written as its decoded double: " + json)
                .isEqualTo("{\"id\":1.2345678901234567E19}");
    }

    @Test
    void preservesFieldOrder() {
        // given
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("zeta", 1L);
        value.put("alpha", 2L);
        value.put("mid", 3L);

        // when
        String json = encode(value);

        // then
        assertThat(json)
                .withFailMessage(() -> "Fields were not written in the map's order: " + json)
                .isEqualTo("{\"zeta\":1,\"alpha\":2,\"mid\":3}");
    }

    @Test
    void writesBytesAsBase64() {
        // when
        String json = encode(List.of(CelByteString.of(new byte[] {1, 2, 3})));

        // then
        assertThat(json)
                .withFailMessage(() -> "Bytes were not written as a base64 string: " + json)
                .isEqualTo("[\"AQID\"]");
    }

    @Test
    void writesAnEmptyStringAsAString() {
        // when
        String json = encode(Map.of("note", ""));

        // then
        assertThat(json)
                .withFailMessage(() -> "An empty string was not written as \"\": " + json)
                .isEqualTo("{\"note\":\"\"}");
    }

    @Test
    void refusesNumbersJsonCannotRepresent() {
        for (Double number : List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            assertThatThrownBy(() -> encoder.encode(Map.of("n", number)))
                    .withFailMessage(() -> number + " was encoded")
                    .isInstanceOf(PayloadEncodeException.class);
        }
    }

    @Test
    void refusesValuesOutsideTheValueTree() {
        Map<Object, Object> numericKey = new HashMap<>();
        numericKey.put(1L, "one");
        List<Object> javaNull = new ArrayList<>();
        javaNull.add(null);

        for (Object value : List.of(new Object(), numericKey, javaNull, Map.of("n", 1))) {
            assertThatThrownBy(() -> encoder.encode(value))
                    .withFailMessage(() -> value + " was encoded")
                    .isInstanceOf(PayloadEncodeException.class);
        }
    }

    @Test
    void writesNullValueAsJsonNull() {
        assertThat(encode(NullValue.NULL_VALUE))
                .withFailMessage(() -> "NullValue was not written as null")
                .isEqualTo("null");
    }

    private Object decode(String json) {
        return decoder.decode(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
    }

    private String encode(Object value) {
        return new String(encoder.encode(value), StandardCharsets.UTF_8);
    }
}
