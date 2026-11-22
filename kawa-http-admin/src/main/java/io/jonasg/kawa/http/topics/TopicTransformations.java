package io.jonasg.kawa.http.topics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.jonasg.kawa.config.TransformationConfig;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.exc.UnrecognizedPropertyException;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/// Reads the `transformations` of a topic request. The steps arrive as raw JSON so that a bad
/// step is reported with its index (`transformations[2] (cel): ...`) instead of a parser message
/// from inside the request body. Steps are read strictly: an unknown key is refused rather than
/// ignored, so a misspelled key cannot silently configure nothing.
final class TopicTransformations {

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();
    /// The step types [TransformationConfig] registers, in declaration order.
    private static final List<String> TYPES = Arrays.stream(TransformationConfig.class.getAnnotation(JsonSubTypes.class).value())
            .map(JsonSubTypes.Type::name)
            .toList();

    private TopicTransformations() {
    }

    /// @throws IllegalArgumentException starting with `transformations[N]` for the first invalid step
    static List<TransformationConfig> parse(List<JsonNode> steps) {
        List<TransformationConfig> transformations = new ArrayList<>(steps.size());
        for (int i = 0; i < steps.size(); i++) {
            transformations.add(step(i, steps.get(i)));
        }
        return transformations;
    }

    private static TransformationConfig step(int index, JsonNode node) {
        String label = "transformations[" + index + "]";
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException(label + ": expected an object with a 'type'");
        }
        JsonNode typeNode = node.get("type");
        if (typeNode == null || !typeNode.isString()) {
            throw new IllegalArgumentException(label + ": 'type' is required");
        }
        String type = typeNode.stringValue();
        if (!TYPES.contains(type)) {
            throw new IllegalArgumentException(label + ": unsupported type '" + type + "', expected one of " + TYPES);
        }
        TransformationConfig step;
        try {
            step = MAPPER.treeToValue(node, TransformationConfig.class);
        } catch (JacksonException e) {
            throw new IllegalArgumentException(label + " (" + type + "): " + reason(e), e);
        }
        // a `null` default is valid, so only the key's presence tells an omitted default apart
        if ("defaultField".equals(type) && !node.has("value")) {
            throw new IllegalArgumentException(label + " (" + type + "): 'value' is required");
        }
        return step;
    }

    /// The step's own validation message when a config record refused it, the parser's otherwise.
    private static String reason(JacksonException e) {
        if (e instanceof UnrecognizedPropertyException unknown) {
            return "unknown property '" + unknown.getPropertyName() + "'";
        }
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof IllegalArgumentException invalid) {
                return invalid.getMessage();
            }
        }
        return e.getOriginalMessage();
    }
}
