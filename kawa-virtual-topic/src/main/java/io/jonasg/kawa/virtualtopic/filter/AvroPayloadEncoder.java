package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Encodes a value tree (see [PayloadEncoder]) as Avro in the schema registry wire format -
/// magic byte `0`, the 4-byte big-endian schema id, then the Avro binary - with the schema
/// registered under a subject (the latest version, or a fixed one).
///
/// The target schema drives the conversion:
///
/// | Avro schema            | Value tree                                                     |
/// |------------------------|----------------------------------------------------------------|
/// | record                 | `Map` with `String` keys                                       |
/// | map                    | `Map` with `String` keys                                       |
/// | array                  | `List`                                                         |
/// | union                  | the first branch the value converts to, exact types first      |
/// | string                 | `String`                                                       |
/// | enum                   | `String` naming one of the symbols                             |
/// | int                    | `Long` within the int range                                    |
/// | long                   | `Long`                                                         |
/// | float, double          | `Double`, or a `Long` (widened, so JSON's `1` fits a double)   |
/// | decimal                | `Double` or `Long`, rounded half-even to the schema's scale    |
/// | boolean                | `Boolean`                                                      |
/// | null                   | `NullValue`                                                    |
/// | bytes, fixed           | [CelByteString] (exactly the fixed size)                       |
///
/// A value that does not match the target schema is resolved the way an Avro reader schema
/// resolves a writer's record: a field the value has but the schema lacks is ignored, and a
/// field the schema has but the value lacks takes the field's default. A missing field without
/// a default, or a value of the wrong type, fails the record with a [PayloadEncodeException]
/// naming the field path.
///
/// Decoding then encoding with the same schema gives the same value tree back, except for
/// decimals: [AvroPayloadDecoder] exposes them as `Double`, so digits beyond double precision
/// are lost.
///
/// A [SchemaRegistryUnavailableException] of the subject resolver passes through unwrapped: a
/// registry outage is not a record error. Any other resolver failure is a [PayloadEncodeException].
public final class AvroPayloadEncoder implements PayloadEncoder {

    private final AvroSubjectResolver subjects;
    private final String subject;
    private final @Nullable Integer version;

    /// @param subjects resolves the target schema on every call, so pass a [CachingAvroSubjectResolver]
    /// @param subject  the registry subject of the target schema
    /// @param version  the subject version, `null` for the latest
    public AvroPayloadEncoder(AvroSubjectResolver subjects, String subject, @Nullable Integer version) {
        this.subjects = subjects;
        this.subject = subject;
        this.version = version;
    }

    @Override
    public byte[] encode(Object value) {
        RegisteredAvroSchema target = target();
        Object datum = toAvro(target.schema(), value, "", true);
        var out = new ByteArrayOutputStream();
        out.write(AvroData.MAGIC_BYTE);
        out.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(target.id()).array());
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            new GenericDatumWriter<>(target.schema(), AvroData.GENERIC).write(datum, encoder);
            encoder.flush();
        } catch (IOException | AvroRuntimeException e) {
            throw new PayloadEncodeException("value cannot be written as Avro with schema id " + target.id() + ": " + e.getMessage(), e);
        }
        return out.toByteArray();
    }

    private RegisteredAvroSchema target() {
        try {
            return subjects.resolve(subject, version);
        } catch (SchemaRegistryUnavailableException e) {
            // An outage, not a bad record: never handled per the onDecodeError policy.
            throw e;
        } catch (RuntimeException e) {
            String versionText = version == null ? "latest" : version.toString();
            throw new PayloadEncodeException("could not resolve Avro subject '" + subject + "' version " + versionText
                    + ": " + e.getMessage(), e);
        }
    }

    /// Converts `value` to the Avro datum `schema` expects. `widen` allows a `Long` where a
    /// float or double is expected; unions first look for a branch without widening.
    private static Object toAvro(Schema schema, Object value, String path, boolean widen) {
        if (value == null) {
            throw new PayloadEncodeException("value tree holds a Java null at '" + display(path) + "', expected NullValue");
        }
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal decimal) {
            return toDecimal(decimal, value, path, schema);
        }
        return switch (schema.getType()) {
            case RECORD -> toRecord(schema, value, path);
            case MAP -> toMap(schema, value, path);
            case ARRAY -> toArray(schema, value, path);
            case UNION -> toUnion(schema, value, path);
            case STRING -> {
                if (value instanceof String text) {
                    yield text;
                }
                throw mismatch(path, value, schema);
            }
            case ENUM -> {
                if (value instanceof String symbol && schema.hasEnumSymbol(symbol)) {
                    yield new GenericData.EnumSymbol(schema, symbol);
                }
                throw mismatch(path, value, schema);
            }
            case INT -> {
                if (value instanceof Long number && number >= Integer.MIN_VALUE && number <= Integer.MAX_VALUE) {
                    yield number.intValue();
                }
                throw mismatch(path, value, schema);
            }
            case LONG -> {
                if (value instanceof Long number) {
                    yield number;
                }
                throw mismatch(path, value, schema);
            }
            case FLOAT -> switch (value) {
                case Double number -> number.floatValue();
                case Long number when widen -> number.floatValue();
                default -> throw mismatch(path, value, schema);
            };
            case DOUBLE -> switch (value) {
                case Double number -> number;
                case Long number when widen -> number.doubleValue();
                default -> throw mismatch(path, value, schema);
            };
            case BOOLEAN -> {
                if (value instanceof Boolean bool) {
                    yield bool;
                }
                throw mismatch(path, value, schema);
            }
            case NULL -> {
                if (value instanceof NullValue) {
                    yield null;
                }
                throw mismatch(path, value, schema);
            }
            case BYTES -> {
                if (value instanceof CelByteString bytes) {
                    yield ByteBuffer.wrap(bytes.toByteArray());
                }
                throw mismatch(path, value, schema);
            }
            case FIXED -> {
                if (value instanceof CelByteString bytes && bytes.size() == schema.getFixedSize()) {
                    yield new GenericData.Fixed(schema, bytes.toByteArray());
                }
                throw mismatch(path, value, schema);
            }
        };
    }

    private static GenericData.Record toRecord(Schema schema, Object value, String path) {
        if (!(value instanceof Map<?, ?> map)) {
            throw mismatch(path, value, schema);
        }
        var record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
            if (map.containsKey(field.name())) {
                record.put(field.pos(), toAvro(field.schema(), map.get(field.name()), fieldPath, true));
            } else if (field.hasDefaultValue()) {
                record.put(field.pos(), AvroData.GENERIC.getDefaultValue(field));
            } else {
                throw new PayloadEncodeException("field '" + fieldPath + "' is missing and has no default in Avro record "
                        + schema.getFullName());
            }
        }
        return record;
    }

    private static Map<String, Object> toMap(Schema schema, Object value, String path) {
        if (!(value instanceof Map<?, ?> map)) {
            throw mismatch(path, value, schema);
        }
        Map<String, Object> avroMap = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new PayloadEncodeException("Avro map keys must be strings, got " + entry.getKey() + " at '" + display(path) + "'");
            }
            avroMap.put(key, toAvro(schema.getValueType(), entry.getValue(), path + "[" + key + "]", true));
        }
        return avroMap;
    }

    private static List<Object> toArray(Schema schema, Object value, String path) {
        if (!(value instanceof List<?> list)) {
            throw mismatch(path, value, schema);
        }
        List<Object> array = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            array.add(toAvro(schema.getElementType(), list.get(i), path + "[" + i + "]", true));
        }
        return array;
    }

    /// The writer picks the union branch from the datum's Java type, so converting to the right
    /// type is enough. Branches matching the value's own type win over widened ones, so a `Long`
    /// in `["double", "long"]` stays a long.
    private static Object toUnion(Schema schema, Object value, String path) {
        for (boolean widen : new boolean[] {false, true}) {
            for (Schema branch : schema.getTypes()) {
                try {
                    return toAvro(branch, value, path, widen);
                } catch (PayloadEncodeException _) {
                    // try the next branch
                }
            }
        }
        throw mismatch(path, value, schema);
    }

    private static BigDecimal toDecimal(LogicalTypes.Decimal decimal, Object value, String path, Schema schema) {
        BigDecimal number = switch (value) {
            case Double d when !d.isNaN() && !d.isInfinite() -> BigDecimal.valueOf(d);
            case Long l -> BigDecimal.valueOf(l);
            default -> throw mismatch(path, value, schema);
        };
        BigDecimal scaled = number.setScale(decimal.getScale(), RoundingMode.HALF_EVEN);
        if (scaled.precision() > decimal.getPrecision()) {
            throw new PayloadEncodeException("value " + number + " at '" + display(path) + "' does not fit decimal("
                    + decimal.getPrecision() + ", " + decimal.getScale() + ")");
        }
        return scaled;
    }

    private static PayloadEncodeException mismatch(String path, Object value, Schema schema) {
        return new PayloadEncodeException("cannot encode " + describe(value) + " at '" + display(path) + "' as Avro " + schemaName(schema));
    }

    /// Scalars with their value, containers by kind only, so a message never dumps a whole record.
    private static String describe(Object value) {
        return switch (value) {
            case NullValue _ -> "null";
            case Map<?, ?> _ -> "an object";
            case List<?> _ -> "an array";
            case String text -> "String \"" + text + "\"";
            default -> value.getClass().getSimpleName() + " " + value;
        };
    }

    private static String schemaName(Schema schema) {
        return switch (schema.getType()) {
            case RECORD, ENUM, FIXED -> schema.getType().getName() + " " + schema.getFullName();
            case UNION -> schema.getTypes().stream().map(AvroPayloadEncoder::schemaName).toList().toString();
            default -> schema.getType().getName();
        };
    }

    private static String display(String path) {
        return path.isEmpty() ? "(root)" : path;
    }
}
