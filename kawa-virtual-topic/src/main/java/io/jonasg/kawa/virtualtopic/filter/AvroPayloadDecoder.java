package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/// Decodes an Avro payload in the schema registry wire format - magic byte `0`, a 4-byte
/// big-endian schema id, then the Avro binary - into CEL-friendly values (see [PayloadDecoder]).
///
/// The decoded record is not copied into a value tree: it is wrapped in an [AvroValues] view
/// that converts a field only when a filter reads it, so a filter that reads one field of a
/// wide record does not pay for converting the rest.
public final class AvroPayloadDecoder implements PayloadDecoder {

    private final AvroSchemaResolver schemaResolver;

    /// @param schemaResolver looks up writer schemas by registry id; it is called for every
    ///                       record, so pass a [CachingAvroSchemaResolver]
    public AvroPayloadDecoder(AvroSchemaResolver schemaResolver) {
        this.schemaResolver = schemaResolver;
    }

    @Override
    public Object decode(ByteBuffer payload) {
        if (payload == null || !payload.hasRemaining()) {
            throw new PayloadDecodeException("record value is empty, expected Avro");
        }
        ByteBuffer buffer = payload.duplicate();
        if (buffer.remaining() < AvroData.HEADER_SIZE || buffer.get() != AvroData.MAGIC_BYTE) {
            throw new PayloadDecodeException("record value is not in the schema registry wire format (missing magic byte 0 and schema id)");
        }
        int schemaId = buffer.getInt();
        Schema schema = schema(schemaId);

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        Object value;
        try {
            value = new GenericDatumReader<>(schema, schema, AvroData.GENERIC).read(null, decoder);
        } catch (IOException | AvroRuntimeException | IndexOutOfBoundsException e) {
            throw new PayloadDecodeException(
                    "record value is not valid Avro for schema id " + schemaId + ": " + e.getMessage(), e);
        }
        return AvroValues.adapt(value);
    }

    private Schema schema(int schemaId) {
        try {
            return schemaResolver.resolve(schemaId);
        } catch (RuntimeException e) {
            throw new PayloadDecodeException(
                    "could not resolve Avro schema id " + schemaId + ": " + e.getMessage(), e);
        }
    }
}
