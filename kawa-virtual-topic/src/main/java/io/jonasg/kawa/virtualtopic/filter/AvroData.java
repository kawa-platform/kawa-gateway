package io.jonasg.kawa.virtualtopic.filter;

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;

/// The Avro data model [AvroPayloadDecoder] and [AvroPayloadEncoder] share.
final class AvroData {

    /// Reads and writes `decimal` logical types as `BigDecimal`; other logical types keep their
    /// underlying Avro type, so timestamps and dates are `Long`.
    static final GenericData GENERIC = new GenericData();

    static {
        GENERIC.addLogicalTypeConversion(new Conversions.DecimalConversion());
    }

    /// The schema registry wire format: magic byte `0`, then a 4-byte big-endian schema id.
    static final byte MAGIC_BYTE = 0;
    static final int HEADER_SIZE = 5;

    private AvroData() {
    }
}
