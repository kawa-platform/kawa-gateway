package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.NullValue;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Decodes a JSON payload into CEL-friendly values (see [PayloadDecoder]). Integral numbers
/// that fit a `long` become `Long`, all other numbers `Double`.
///
/// A field missing from a JSON object reads as `null`, so `value.status == "PAID"` is `false`
/// for a document without `status` instead of a CEL "no such key" error failing the fetch.
/// Presence can still be tested explicitly with `has(value.status)`.
public final class JsonPayloadDecoder implements PayloadDecoder {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Override
    public Object decode(ByteBuffer payload) {
        if (payload == null || !payload.hasRemaining()) {
            throw new PayloadDecodeException("record value is empty, expected JSON");
        }
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        JsonNode root;
        try {
            root = MAPPER.readTree(bytes);
        } catch (JacksonException e) {
            throw new PayloadDecodeException("record value is not valid JSON: " + e.getOriginalMessage(), e);
        }
        if (root == null || root.isMissingNode()) {
            throw new PayloadDecodeException("record value is empty, expected JSON");
        }
        return toValue(root);
    }

    private static Object toValue(JsonNode node) {
        if (node.isObject()) {
            Map<String, Object> object = new JsonObject();
            for (Map.Entry<String, JsonNode> property : node.properties()) {
                object.put(property.getKey(), toValue(property.getValue()));
            }
            return object;
        }
        if (node.isArray()) {
            List<Object> array = new ArrayList<>(node.size());
            for (JsonNode element : node) {
                array.add(toValue(element));
            }
            return array;
        }
        if (node.isString()) {
            return node.stringValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isIntegralNumber() && node.canConvertToLong()) {
            return node.longValue();
        }
        if (node.isNumber()) {
            return node.doubleValue();
        }
        return NullValue.NULL_VALUE;
    }

    /// A JSON object whose absent fields read as `null` rather than raising a CEL error.
    static final class JsonObject extends LinkedHashMap<String, Object> {

        @Override
        public Object get(Object key) {
            return containsKey(key) ? super.get(key) : NullValue.NULL_VALUE;
        }
    }
}
