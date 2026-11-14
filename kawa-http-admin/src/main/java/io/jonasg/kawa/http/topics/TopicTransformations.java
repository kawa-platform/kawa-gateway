package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.TransformationConfig;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/// Reads the `transformations` of a topic request. The steps arrive as raw JSON so that a bad
/// step is reported with its index (`transformations[2] (cel): ...`) instead of a parser message
/// from inside the request body.
final class TopicTransformations {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final Set<String> TYPES = Set.of("dropFields", "renameField", "defaultField", "cel", "convert");

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

    /// Refuses a `convert` step that is not the last one, naming it.
    static void requireConvertLast(List<TransformationConfig> transformations) {
        for (int i = 0; i < transformations.size() - 1; i++) {
            if (transformations.get(i) instanceof ConvertTransformationConfig) {
                throw new IllegalArgumentException("transformations[" + i + "] (convert): must be the last step");
            }
        }
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
            throw new IllegalArgumentException(label + ": unsupported type '" + type + "', expected one of "
                    + List.of("dropFields", "renameField", "defaultField", "cel", "convert"));
        }
        try {
            return MAPPER.treeToValue(node, TransformationConfig.class);
        } catch (JacksonException e) {
            throw new IllegalArgumentException(label + " (" + type + "): " + reason(e), e);
        }
    }

    /// The step's own validation message when a config record refused it, the parser's otherwise.
    private static String reason(JacksonException e) {
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof IllegalArgumentException invalid) {
                return invalid.getMessage();
            }
        }
        return e.getOriginalMessage();
    }
}
