package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/// (De)serializes Kafka request/response bodies using kafka-clients' generated message
/// classes. Only APIs registered in the [KafkaApiRegistry] are decoded; everything
/// else is forwarded raw.
public final class KafkaBodyCodec {

    private final KafkaApiRegistry registry;

    public KafkaBodyCodec(KafkaApiRegistry registry) {
        this.registry = registry;
    }

    /// Decodes a request body from `buf`. On return the buffer is fully consumed.
    /// Returns `null` when the api is not registered for decoding.
    public Object decodeRequest(
            short apiKey,
            short version,
            ByteBuf buf
    ) {
        return decode(apiKey, version, buf, true);
    }

    public Object decodeResponse(
            short apiKey,
            short version,
            ByteBuf buf
    ) {
        return decode(apiKey, version, buf, false);
    }

    private Object decode(
            short apiKey,
            short version,
            ByteBuf buf,
            boolean request
    ) {
        KafkaApiSpec spec = registry.spec(apiKey);
        MessageReader reader = spec == null || !spec.versionRange().contains(version)
                ? null
                : request ? spec.requestReader() : spec.responseReader();
        if (reader == null) {
            return null;
        }
        var readable = new ByteBufReadable(buf);
        Message message = reader.read(readable, version);
        buf.readerIndex(buf.readerIndex() + readable.consumed());
        return message;
    }

    public void encodeRequest(
            short version,
            Object body,
            ByteBuf out
    ) {
        encode(version, body, out);
    }

    public void encodeResponse(
            short version,
            Object body,
            ByteBuf out
    ) {
        encode(version, body, out);
    }

    public int bodySize(
            short version,
            Object body
    ) {
        Message message = (Message) body;
        var cache = new ObjectSerializationCache();
        var accumulator = new MessageSizeAccumulator();
        message.addSize(accumulator, cache, version);
        return accumulator.totalSize();
    }

    private void encode(
            short version,
            Object body,
            ByteBuf out
    ) {
        Message message = (Message) body;
        var cache = new ObjectSerializationCache();
        var accumulator = new MessageSizeAccumulator();
        message.addSize(accumulator, cache, version);
        int size = accumulator.totalSize();
        int start = out.writerIndex();
        message.write(new ByteBufWritable(out), cache, version);
        int written = out.writerIndex() - start;
        if (written != size) {
            throw new IllegalStateException(
                    "Encoded size mismatch: expected " + size + " bytes, wrote " + written);
        }
    }
}
