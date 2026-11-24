package io.jonasg.kawa.virtualtopic.mapping;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import io.jonasg.kawa.virtualtopic.filter.NullDefaultingMap;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/// Converts values from outside the value tree - CEL results and JSON literals from the
/// configuration - into it: `Map<String, Object>` (absent keys read as `null`), `List<Object>`,
/// `String`, `Long`, `Double`, `Boolean`, `NullValue` and [CelByteString].
final class ValueTree {

    private ValueTree() {
    }

    /// @throws PayloadMappingException if `value` holds something the value tree cannot
    static Object of(Object value) {
        return switch (value) {
            case null -> NullValue.NULL_VALUE;
            case Map<?, ?> map -> {
                Map<String, Object> object = new NullDefaultingMap();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String name)) {
                        throw new PayloadMappingException("object keys must be strings, got " + typeName(entry.getKey()));
                    }
                    object.put(name, of(entry.getValue()));
                }
                yield object;
            }
            case Collection<?> collection -> {
                List<Object> list = new ArrayList<>(collection.size());
                collection.forEach(element -> list.add(of(element)));
                yield list;
            }
            case String _, Long _, Double _, Boolean _, NullValue _, CelByteString _ -> value;
            case Integer number -> number.longValue();
            case Short number -> number.longValue();
            case Float number -> Double.parseDouble(number.toString());
            case BigDecimal number -> number.doubleValue();
            case BigInteger number when number.bitLength() < Long.SIZE -> number.longValue();
            // e.g. CEL's uint, timestamp and duration: convert with int(), string() or similar first
            default -> throw new PayloadMappingException("cannot map a " + value.getClass().getSimpleName() + " into a record value");
        };
    }

    private static String typeName(@Nullable Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }
}
