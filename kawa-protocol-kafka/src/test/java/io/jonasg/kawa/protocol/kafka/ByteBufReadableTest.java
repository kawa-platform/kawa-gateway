package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ByteBufReadableTest {

    @Test
    void readsUnsignedVarintsInKafkaWireFormat() {
        // given
        var readable = new ByteBufReadable(bytes("00" + "ac02" + "ffffffff0f"));

        // when
        int[] values = {readable.readUnsignedVarint(), readable.readUnsignedVarint(), readable.readUnsignedVarint()};

        // then
        assertThat(values)
                .withFailMessage(() -> "Kafka's unsigned varints were not read as 0, 300 and -1: " + Arrays.toString(values))
                .containsExactly(0, 300, -1);
    }

    @Test
    void readsZigZagVarintsAndVarlongsInKafkaWireFormat() {
        // given
        var readable = new ByteBufReadable(bytes("01" + "ffffffff0f" + "ffffffffffffffffff01"));

        // when
        int minusOne = readable.readVarint();
        int min = readable.readVarint();
        long minLong = readable.readVarlong();

        // then
        assertThat(new long[]{minusOne, min, minLong})
                .withFailMessage(() -> "Kafka's varints and varlong were not read as -1, Integer.MIN_VALUE and Long.MIN_VALUE")
                .containsExactly(-1, Integer.MIN_VALUE, Long.MIN_VALUE);
    }

    @Test
    void countsTheVarintBytesItConsumed() {
        // given
        var readable = new ByteBufReadable(bytes("ac02" + "01" + "7f"));

        // when
        readable.readUnsignedVarint();
        readable.readVarint();

        // then
        assertThat(readable.consumed())
                .withFailMessage(() -> "The three varint bytes read were not counted as consumed: " + readable.consumed())
                .isEqualTo(3);
        assertThat(readable.remaining())
                .withFailMessage(() -> "The unread byte was not left remaining: " + readable.remaining())
                .isEqualTo(1);
    }

    @Test
    void rejectsAVarintLongerThanFiveBytes() {
        // given
        var readable = new ByteBufReadable(bytes("ffffffffff01"));

        // when / then
        assertThatThrownBy(readable::readVarint)
                .withFailMessage(() -> "A six-byte varint was accepted")
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ByteBuf bytes(String hex) {
        return Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hex));
    }
}
