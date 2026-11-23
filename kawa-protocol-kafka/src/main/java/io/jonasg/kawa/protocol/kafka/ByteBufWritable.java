package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Writable;

import java.nio.ByteBuffer;

/// Adapter writing kafka-clients' `Writable` output into a Netty `ByteBuf`. Varints are encoded by Kafka's own
/// [ByteBufferAccessor] into a scratch buffer and then copied over.
public final class ByteBufWritable implements Writable {

    private final ByteBuf out;

    public ByteBufWritable(ByteBuf out) {
        this.out = out;
    }

    @Override
    public void writeByte(byte value) {
        out.writeByte(value);
    }

    @Override
    public void writeShort(short value) {
        out.writeShort(value);
    }

    @Override
    public void writeInt(int value) {
        out.writeInt(value);
    }

    @Override
    public void writeLong(long value) {
        out.writeLong(value);
    }

    @Override
    public void writeDouble(double value) {
        out.writeDouble(value);
    }

    @Override
    public void writeByteArray(byte[] value) {
        out.writeBytes(value);
    }

    @Override
    public void writeUnsignedVarint(int value) {
        ByteBuffer scratch = ByteBuffer.allocate(5);
        new ByteBufferAccessor(scratch).writeUnsignedVarint(value);
        out.writeBytes(scratch.flip());
    }

    @Override
    public void writeByteBuffer(ByteBuffer value) {
        out.writeBytes(value);
    }

    @Override
    public void writeVarint(int value) {
        ByteBuffer scratch = ByteBuffer.allocate(5);
        new ByteBufferAccessor(scratch).writeVarint(value);
        out.writeBytes(scratch.flip());
    }

    @Override
    public void writeVarlong(long value) {
        ByteBuffer scratch = ByteBuffer.allocate(10);
        new ByteBufferAccessor(scratch).writeVarlong(value);
        out.writeBytes(scratch.flip());
    }
}
