package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager.VirtualTopic;
import io.jonasg.kawa.virtualtopic.filter.AvroSchemaResolver;
import io.jonasg.kawa.virtualtopic.filter.AvroSubjectResolver;
import io.jonasg.kawa.virtualtopic.filter.PayloadCodecs;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecodeException;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.PayloadEncodeException;
import io.jonasg.kawa.virtualtopic.filter.PayloadEncoder;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Applies a virtual topic's transformations to a fetched partition's records: each record value
/// is decoded with the topic's `valueFormat`, mapped, and encoded in its output format, and every
/// batch is rebuilt around the new values.
///
/// Batches are rebuilt the way Kafka's own `MemoryRecords.filterTo` rebuilds the batches it
/// filters (log compaction): offsets, timestamps, keys and headers stay, as do the producer id,
/// epoch and base sequence, the transactional flag and the partition leader epoch; the batch keeps
/// its last offset, so a consumer moves past it even when its last records were dropped, and its
/// compression codec. Batches in the legacy formats (magic v0 and v1) come out as v2. So
/// `read_committed` consumers still drop aborted records (the aborted transactions of the
/// partition name producer ids and offsets, both preserved), and control batches - transaction
/// markers - are copied unchanged.
///
/// A trailing batch the broker cut off at the fetch size is not a complete batch, so it is left
/// out, as `filterTo` leaves it out; the consumer fetches it again from its position.
///
/// A tombstone (a record without a value) passes through unchanged. A record whose value cannot be
/// transformed - it does not decode, the mapping fails on it, or the result cannot be encoded - is
/// handled per the `onDecodeError` policy of the topic's `valueFormat`:
///
/// | Policy    | The consumer gets                                                                  |
/// |-----------|------------------------------------------------------------------------------------|
/// | `skip`    | nothing: the record is dropped, leaving an offset gap (the default)                |
/// | `include` | the stored value, untransformed - visibly not in the format it expects            |
/// | `fail`    | no response: the fetch fails, as a `fail` filter does                              |
///
/// A schema registry outage ([io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException]) is not a record error and
/// bypasses the policy: it is thrown, whatever the policy, so the partition fails instead of
/// records being dropped or delivered untransformed.
///
/// Every record of a transformed topic is decoded, mapped and re-encoded on every fetch: the cost
/// grows with the number and size of the records, and it is paid again by every consumer.
public final class VirtualTopicRecordTransformer {

    private static final Logger log = LoggerFactory.getLogger(VirtualTopicRecordTransformer.class);

    /// Codecs keyed by formats and the cluster's schema registry, so they are built once per
    /// distinct configuration rather than for every fetched partition. Resolvers compare by identity.
    private final Map<CodecKey, Codecs> codecs = new ConcurrentHashMap<>();

    private final SchemaRegistries schemaRegistries;

    /// @param schemaRegistries finds the schema registry of a physical topic's cluster, to decode
    ///                         and encode Avro values with
    public VirtualTopicRecordTransformer(SchemaRegistries schemaRegistries) {
        this.schemaRegistries = schemaRegistries;
    }

    /// Returns `records` of `topic` with every value transformed, or `records` unchanged when
    /// `topic` has no transformations or there are no records (the fast path).
    ///
    /// @throws RuntimeException when a record cannot be transformed and the policy is `fail`
    /// @throws io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException when the schema registry is unavailable, whatever the policy
    public BaseRecords apply(VirtualTopic topic, BaseRecords records) {
        if (!topic.transformsValues() || !(records instanceof MemoryRecords memoryRecords) || memoryRecords.sizeInBytes() == 0) {
            return records;
        }
        Transformation transformation = new Transformation(codecsFor(topic), topic.mapping(), topic.valueFormat().onDecodeError());
        var output = new ByteBufferOutputStream(memoryRecords.sizeInBytes());
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            if (batch.isControlBatch()) {
                batch.writeTo(output);
            } else {
                transformBatch(batch, transformation, output);
            }
        }
        ByteBuffer buffer = output.buffer();
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private Codecs codecsFor(VirtualTopic topic) {
        PayloadFormatConfig valueFormat = topic.valueFormat();
        PayloadFormatConfig outputFormat = topic.outputFormat();
        var key = new CodecKey(valueFormat, outputFormat,
                schemaRegistries.avroSchemasFor(topic.physical()), schemaRegistries.avroSubjectsFor(topic.physical()));
        return codecs.computeIfAbsent(key, k -> new Codecs(
                PayloadCodecs.decoder(k.valueFormat(), k.avroSchemas()),
                PayloadCodecs.encoder(k.outputFormat(), k.avroSubjects())));
    }

    private static void transformBatch(MutableRecordBatch batch, Transformation transformation, ByteBufferOutputStream output) {
        List<TransformedRecord> kept = new ArrayList<>();
        try (CloseableIterator<Record> records = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
            while (records.hasNext()) {
                Record record = records.next();
                if (!record.hasValue()) {
                    kept.add(new TransformedRecord(record, null));
                    continue;
                }
                ByteBuffer value = transformation.apply(record);
                if (value != null) {
                    kept.add(new TransformedRecord(record, value));
                }
            }
        }
        if (kept.isEmpty()) {
            writeEmptyBatch(batch, output);
        } else {
            writeBatch(batch, kept, output);
        }
    }

    /// Keeps a batch whose records were all dropped as an empty batch, as `filterTo` does, so the
    /// consumer still moves past its offsets and producer state is kept. Legacy batches cannot be
    /// empty, so they are left out.
    private static void writeEmptyBatch(RecordBatch batch, ByteBufferOutputStream output) {
        if (batch.magic() < RecordBatch.MAGIC_VALUE_V2) {
            return;
        }
        output.ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(output.buffer(), RecordBatch.CURRENT_MAGIC_VALUE, batch.producerId(),
                batch.producerEpoch(), batch.baseSequence(), batch.baseOffset(), batch.lastOffset(),
                batch.partitionLeaderEpoch(), batch.timestampType(), batch.maxTimestamp(),
                batch.isTransactional(), batch.isControlBatch());
    }

    /// Writes `records` as a batch with the metadata of `original`, mirroring `filterTo`'s
    /// `buildRetainedRecordsInto`.
    private static void writeBatch(RecordBatch original, List<TransformedRecord> records, ByteBufferOutputStream output) {
        boolean legacy = original.magic() < RecordBatch.MAGIC_VALUE_V2;
        // v0 has no timestamp type: CREATE_TIME without timestamps, as filterTo up-converts it
        TimestampType timestampType = original.timestampType() == TimestampType.NO_TIMESTAMP_TYPE
                ? TimestampType.CREATE_TIME
                : original.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? original.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
        long baseOffset = legacy ? records.getFirst().record().offset() : original.baseOffset();
        try (var builder = new MemoryRecordsBuilder(output, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.of(original.compressionType()).build(), timestampType, baseOffset, logAppendTime,
                original.producerId(), original.producerEpoch(), original.baseSequence(), original.isTransactional(),
                false, original.partitionLeaderEpoch(), output.limit(),
                original.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP))) {
            for (TransformedRecord transformed : records) {
                Record record = transformed.record();
                builder.appendWithOffset(record.offset(), record.timestamp(), record.key(), transformed.value(), record.headers());
            }
            if (!legacy) {
                builder.overrideLastOffset(original.lastOffset());
            }
            builder.build();
        }
    }

    private record Transformation(Codecs codecs, ValueMapping mapping, DecodeErrorPolicy onError) {

        /// The transformed value of `record`; on failure its stored value or `null` (dropped) per
        /// `onError`. A [io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException] is not caught: it is no record error.
        @Nullable ByteBuffer apply(Record record) {
            try {
                Object value = codecs.decoder().decode(record.value());
                return ByteBuffer.wrap(codecs.encoder().encode(mapping.apply(value, record)));
            } catch (PayloadDecodeException | PayloadMappingException | PayloadEncodeException e) {
                return switch (onError) {
                    case SKIP -> {
                        log.debug("Dropping record at offset {}, its value cannot be transformed: {}", record.offset(), e.getMessage());
                        yield null;
                    }
                    case INCLUDE -> {
                        log.debug("Delivering record at offset {} untransformed: {}", record.offset(), e.getMessage());
                        yield record.value();
                    }
                    case FAIL -> throw e;
                };
            }
        }
    }

    private record TransformedRecord(Record record, @Nullable ByteBuffer value) {
    }

    private record Codecs(PayloadDecoder decoder, PayloadEncoder encoder) {
    }

    private record CodecKey(
            PayloadFormatConfig valueFormat,
            PayloadFormatConfig outputFormat,
            @Nullable AvroSchemaResolver avroSchemas,
            @Nullable AvroSubjectResolver avroSubjects
    ) {
    }
}
