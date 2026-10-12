package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroPayloadDecoderTest {

    private static final Schema STRING = Schema.create(Schema.Type.STRING);

    @Test
    void decodesAPrimitiveTopLevelValue() {
        // given
        var decoder = new AvroPayloadDecoder(id -> STRING);

        // when
        Object value = decoder.decode(ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, STRING, "hello")));

        // then
        assertThat(value)
                .withFailMessage(() -> "Top-level Avro string was not decoded to a Java String, got " + value)
                .isEqualTo("hello");
    }

    @Test
    void doesNotConsumeThePayloadBuffer() {
        // given
        var decoder = new AvroPayloadDecoder(id -> STRING);
        var payload = ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(1, STRING, "hello"));
        int position = payload.position();

        // when
        decoder.decode(payload);

        // then
        assertThat(payload.position())
                .withFailMessage(() -> "Decoding moved the record value's buffer position")
                .isEqualTo(position);
    }

    @Test
    void rejectsAnEmptyPayload() {
        // given
        var decoder = new AvroPayloadDecoder(id -> STRING);

        // when / then
        assertThatThrownBy(() -> decoder.decode(null))
                .withFailMessage(() -> "A null record value was not rejected as undecodable")
                .isInstanceOf(PayloadDecodeException.class);
        assertThatThrownBy(() -> decoder.decode(ByteBuffer.allocate(0)))
                .withFailMessage(() -> "An empty record value was not rejected as undecodable")
                .isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    void rejectsAPayloadWithoutTheMagicByte() {
        // given
        var decoder = new AvroPayloadDecoder(id -> STRING);
        byte[] payload = AvroRecordPredicateTest.wireFormat(1, STRING, "hello");
        payload[0] = 1;

        // when / then
        assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(payload)))
                .withFailMessage(() -> "A record value without magic byte 0 was not rejected")
                .isInstanceOf(PayloadDecodeException.class)
                .hasMessageContaining("wire format");
    }

    @Test
    void rejectsAnUnresolvableSchemaId() {
        // given
        var decoder = new AvroPayloadDecoder(id -> {
            throw new IllegalArgumentException("schema " + id + " not found");
        });

        // when / then
        assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(AvroRecordPredicateTest.wireFormat(9, STRING, "hello"))))
                .withFailMessage(() -> "An unknown schema id was not reported as a decode error")
                .isInstanceOf(PayloadDecodeException.class)
                .hasMessageContaining("schema id 9");
    }

    @Test
    void rejectsATruncatedBody() {
        // given
        var schema = Schema.createRecord("Pair", null, "test", false, java.util.List.of(
                new Schema.Field("a", Schema.create(Schema.Type.LONG)),
                new Schema.Field("b", STRING)));
        var decoder = new AvroPayloadDecoder(id -> schema);
        byte[] header = {0, 0, 0, 0, 1};

        // when / then
        assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(header)))
                .withFailMessage(() -> "A record value with a header but no Avro body was not rejected")
                .isInstanceOf(PayloadDecodeException.class);
    }
}
