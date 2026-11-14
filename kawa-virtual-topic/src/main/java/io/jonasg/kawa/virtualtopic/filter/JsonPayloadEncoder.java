package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/// Encodes a value tree as JSON (see [PayloadEncoder]), the inverse of [JsonPayloadDecoder].
///
/// | Value tree              | JSON                                                   |
/// |-------------------------|--------------------------------------------------------|
/// | `Map<String, Object>`   | object, fields in the map's iteration order            |
/// | `List<Object>`          | array                                                  |
/// | `String`                | string (`""` stays distinct from `null`)               |
/// | `Long`                  | integer                                                |
/// | `Double`                | number with a fraction or exponent (`1.0`, not `1`)    |
/// | `Boolean`               | `true` / `false`                                       |
/// | `NullValue`             | `null`                                                 |
/// | [CelByteString]         | base64 string                                          |
///
/// Only the entries a map holds are written, so an explicit `null` field stays present and an
/// absent field stays absent. A `Double` keeps its fraction so it decodes back to a `Double`
/// rather than a `Long`. NaN and infinities have no JSON form and are refused.
///
/// Decoding then encoding JSON preserves every value except numbers a `Double` cannot hold
/// exactly: [JsonPayloadDecoder] already reads an integer beyond `long` (and a decimal beyond
/// double precision) as a `Double`, so `12345678901234567890` is written back as
/// `1.2345678901234567E19`. Whitespace and number spelling are normalized (`1e2` becomes
/// `100.0`). Bytes are one-way: their base64 string decodes back as a `String`.
public final class JsonPayloadEncoder implements PayloadEncoder {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    @Override
    public byte[] encode(Object value) {
        return MAPPER.writeValueAsBytes(toNode(value));
    }

    private static JsonNode toNode(Object value) {
        return switch (value) {
            case Map<?, ?> map -> toObject(map);
            case List<?> list -> toArray(list);
            case String text -> NODES.stringNode(text);
            case Long number -> NODES.numberNode(number);
            case Double number -> toNumber(number);
            case Boolean bool -> NODES.booleanNode(bool);
            case NullValue _ -> NODES.nullNode();
            case CelByteString bytes -> NODES.stringNode(Base64.getEncoder().encodeToString(bytes.toByteArray()));
            case null -> throw new PayloadEncodeException("value tree holds a Java null, expected NullValue");
            default -> throw new PayloadEncodeException("cannot encode " + value.getClass().getName() + " as JSON");
        };
    }

    private static ObjectNode toObject(Map<?, ?> map) {
        ObjectNode object = NODES.objectNode();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String name)) {
                throw new PayloadEncodeException("JSON object keys must be strings, got " + entry.getKey());
            }
            object.set(name, toNode(entry.getValue()));
        }
        return object;
    }

    private static ArrayNode toArray(List<?> list) {
        ArrayNode array = NODES.arrayNode(list.size());
        for (Object element : list) {
            array.add(toNode(element));
        }
        return array;
    }

    private static JsonNode toNumber(Double number) {
        if (number.isNaN() || number.isInfinite()) {
            throw new PayloadEncodeException("cannot encode " + number + " as JSON");
        }
        return NODES.numberNode(number);
    }
}
