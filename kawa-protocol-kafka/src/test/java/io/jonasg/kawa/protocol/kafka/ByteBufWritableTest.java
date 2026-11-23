package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ByteBufWritableTest {

    @Test
    void writesUnsignedVarintsInKafkaWireFormat() {
        // given
        ByteBuf out = Unpooled.buffer();
        var writable = new ByteBufWritable(out);

        // when
        writable.writeUnsignedVarint(0);
        writable.writeUnsignedVarint(300);
        writable.writeUnsignedVarint(-1);

        // then
        assertThat(ByteBufUtil.hexDump(out))
                .withFailMessage(() -> "Unsigned varints 0, 300 and -1 were not written as Kafka encodes them: " + ByteBufUtil.hexDump(out))
                .isEqualTo("00" + "ac02" + "ffffffff0f");
    }

    @Test
    void writesZigZagVarintsInKafkaWireFormat() {
        // given
        ByteBuf out = Unpooled.buffer();
        var writable = new ByteBufWritable(out);

        // when
        writable.writeVarint(-1);
        writable.writeVarint(1);
        writable.writeVarint(Integer.MIN_VALUE);

        // then
        assertThat(ByteBufUtil.hexDump(out))
                .withFailMessage(() -> "Varints -1, 1 and Integer.MIN_VALUE were not written as Kafka encodes them: " + ByteBufUtil.hexDump(out))
                .isEqualTo("01" + "02" + "ffffffff0f");
    }

    @Test
    void writesZigZagVarlongsInKafkaWireFormat() {
        // given
        ByteBuf out = Unpooled.buffer();
        var writable = new ByteBufWritable(out);

        // when
        writable.writeVarlong(-1L);
        writable.writeVarlong(Long.MIN_VALUE);

        // then
        assertThat(ByteBufUtil.hexDump(out))
                .withFailMessage(() -> "Varlongs -1 and Long.MIN_VALUE were not written as Kafka encodes them: " + ByteBufUtil.hexDump(out))
                .isEqualTo("01" + "ffffffffffffffffff01");
    }
}
