package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Readable;

import java.nio.ByteBuffer;

/// Adapter exposing a Netty `ByteBuf` through kafka-clients' `Readable`. After
/// the message has been read, call `consumed()` and advance the buffer's reader index
/// by that amount.
///
/// Varints are decoded by Kafka's own [ByteBufferAccessor] over the same buffer, so they share its position.
/// Everything else stays here: [#readByteBuffer(int)] in particular copies, because the `ByteBuf` is released
/// once the message is decoded.
public final class ByteBufReadable implements Readable {

    private final ByteBuffer buffer;
    private final ByteBufferAccessor varints;

    public ByteBufReadable(ByteBuf buf) {
        this(buf.nioBuffer(buf.readerIndex(), buf.readableBytes()));
    }

    private ByteBufReadable(ByteBuffer buffer) {
        this.buffer = buffer;
        this.varints = new ByteBufferAccessor(buffer);
    }

    @Override
    public Readable slice() {
        return new ByteBufReadable(buffer.slice());
    }

    @Override
    public byte readByte() {
        return buffer.get();
    }

    @Override
    public short readShort() {
        return buffer.getShort();
    }

    @Override
    public int readInt() {
        return buffer.getInt();
    }

    @Override
    public long readLong() {
        return buffer.getLong();
    }

    @Override
    public double readDouble() {
        return buffer.getDouble();
    }

    @Override
    public byte[] readArray(int size) {
        byte[] array = new byte[size];
        buffer.get(array);
        return array;
    }

    @Override
    public int readUnsignedVarint() {
        return varints.readUnsignedVarint();
    }

    @Override
    public ByteBuffer readByteBuffer(int size) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public int readVarint() {
        return varints.readVarint();
    }

    @Override
    public long readVarlong() {
        return varints.readVarlong();
    }

    @Override
    public int remaining() {
        return buffer.remaining();
    }

    public int consumed() {
        return buffer.position();
    }
}
