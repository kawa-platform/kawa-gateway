package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;

/// Read-only views over decoded Avro data that present it as the value tree [PayloadDecoder]
/// promises, converting each value only when it is read.
///
/// | Avro                       | CEL value                          |
/// |----------------------------|------------------------------------|
/// | record                     | `Map<String, Object>` ([RecordMap]) |
/// | map                        | `Map<String, Object>` ([AvroMap])   |
/// | array                      | `List<Object>` ([AvroList])         |
/// | string, enum               | `String`                           |
/// | int, long                  | `Long`                             |
/// | float, double, decimal     | `Double`                           |
/// | boolean                    | `Boolean`                          |
/// | bytes, fixed               | [CelByteString]                    |
/// | null                       | [NullValue#NULL_VALUE]             |
///
/// Unions need no handling: the reader already yields the value of the branch that was written.
/// Like [JsonPayloadDecoder], a missing record field or map key reads as `null` rather than
/// raising a CEL error; presence can be tested with `has(value.field)`.
final class AvroValues {

    private AvroValues() {
    }

    static Object adapt(Object value) {
        return switch (value) {
            case null -> NullValue.NULL_VALUE;
            case GenericRecord record -> new RecordMap(record);
            case Map<?, ?> map -> new AvroMap(map);
            case List<?> list -> new AvroList(list);
            case CharSequence text -> text.toString();
            case GenericEnumSymbol<?> symbol -> symbol.toString();
            case Integer number -> number.longValue();
            // Via the decimal string, so a float 9.99 compares equal to the CEL literal 9.99
            // instead of reading as 9.989999771118164.
            case Float number -> Double.parseDouble(number.toString());
            case BigDecimal number -> number.doubleValue();
            case ByteBuffer bytes -> CelByteString.of(toArray(bytes));
            case GenericFixed fixed -> CelByteString.of(fixed.bytes().clone());
            default -> value; // Long, Double, Boolean
        };
    }

    private static byte[] toArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /// An Avro record as a map of its fields.
    static final class RecordMap extends AbstractMap<String, Object> {

        private final GenericRecord record;

        RecordMap(GenericRecord record) {
            this.record = record;
        }

        @Override
        public Object get(Object key) {
            Schema.Field field = key instanceof String name ? record.getSchema().getField(name) : null;
            return field == null ? NullValue.NULL_VALUE : adapt(record.get(field.pos()));
        }

        @Override
        public boolean containsKey(Object key) {
            return key instanceof String name && record.getSchema().getField(name) != null;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            List<Schema.Field> fields = record.getSchema().getFields();
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    Iterator<Schema.Field> it = fields.iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Schema.Field field = it.next();
                            return new SimpleImmutableEntry<>(field.name(), adapt(record.get(field.pos())));
                        }
                    };
                }

                @Override
                public int size() {
                    return fields.size();
                }
            };
        }
    }

    /// An Avro map. The reader keys it by [Utf8] (or `String` for schemas with
    /// `avro.java.string: String`), so lookups try both.
    static final class AvroMap extends AbstractMap<String, Object> {

        private final Map<?, ?> map;

        AvroMap(Map<?, ?> map) {
            this.map = map;
        }

        @Override
        public Object get(Object key) {
            Object rawKey = rawKey(key);
            return rawKey == null ? NullValue.NULL_VALUE : adapt(map.get(rawKey));
        }

        @Override
        public boolean containsKey(Object key) {
            return rawKey(key) != null;
        }

        private Object rawKey(Object key) {
            if (!(key instanceof String name)) {
                return null;
            }
            if (map.containsKey(name)) {
                return name;
            }
            var utf8 = new Utf8(name);
            return map.containsKey(utf8) ? utf8 : null;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    Iterator<? extends Entry<?, ?>> it = map.entrySet().iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Entry<?, ?> entry = it.next();
                            return new SimpleImmutableEntry<>(entry.getKey().toString(), adapt(entry.getValue()));
                        }
                    };
                }

                @Override
                public int size() {
                    return map.size();
                }
            };
        }
    }

    /// An Avro array.
    static final class AvroList extends AbstractList<Object> implements RandomAccess {

        private final List<?> list;

        AvroList(List<?> list) {
            this.list = list;
        }

        @Override
        public Object get(int index) {
            return adapt(list.get(index));
        }

        @Override
        public int size() {
            return list.size();
        }
    }
}
