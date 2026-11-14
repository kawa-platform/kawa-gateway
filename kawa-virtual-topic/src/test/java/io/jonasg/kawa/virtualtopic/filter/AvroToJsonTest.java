package io.jonasg.kawa.virtualtopic.filter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/// Converting Avro to JSON: the [AvroPayloadDecoder] view encoded by [JsonPayloadEncoder].
class AvroToJsonTest {

    private final AvroPayloadDecoder avro = new AvroPayloadDecoder(id -> AvroPayloadEncoderTest.ORDER);
    private final JsonPayloadEncoder json = new JsonPayloadEncoder();

    @Test
    void writesEveryAvroTypeAsJson() {
        // given
        Object order = avro.decode(ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, AvroPayloadEncoderTest.ORDER, AvroPayloadEncoderTest.order())));

        // when
        String text = new String(json.encode(order), StandardCharsets.UTF_8);

        // then
        assertThat(text)
                .withFailMessage(() -> "The Avro order was not converted to the expected JSON: " + text)
                .isEqualTo("{\"id\":42,\"qty\":3,\"status\":\"PAID\",\"customer\":{\"name\":\"Zoë 東京 🚀\"},"
                        + "\"lines\":[{\"sku\":\"A\",\"price\":9.99}],\"tags\":{\"channel\":\"web\"},\"note\":null,"
                        + "\"weight\":1.5,\"paid\":true,\"total\":12.34,\"signature\":\"AQID\",\"checksum\":\"3q2+7w==\","
                        + "\"createdAt\":1700000000000}");
    }

    @Test
    void leavesADroppedFieldOutOfTheJson() {
        // given
        @SuppressWarnings("unchecked")
        Map<String, Object> order = new LinkedHashMap<>((Map<String, Object>) avro.decode(
                ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, AvroPayloadEncoderTest.ORDER, AvroPayloadEncoderTest.order()))));
        order.keySet().retainAll(Set.of("id", "status"));

        // when
        String text = new String(json.encode(order), StandardCharsets.UTF_8);

        // then
        assertThat(text)
                .withFailMessage(() -> "Dropped Avro fields were written to the JSON: " + text)
                .isEqualTo("{\"id\":42,\"status\":\"PAID\"}");
    }
}
