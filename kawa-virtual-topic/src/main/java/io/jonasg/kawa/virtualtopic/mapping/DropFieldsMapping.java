package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.FieldPath;
import org.apache.kafka.common.record.internal.Record;

import java.util.List;

/// Removes fields; a list on a path drops the field from every element. A path the value does
/// not have is ignored.
final class DropFieldsMapping implements ValueMapping {

    private final List<List<String>> paths;

    DropFieldsMapping(DropFieldsTransformationConfig config) {
        this.paths = config.fields().stream().map(FieldPath::parse).toList();
    }

    @Override
    public Object apply(Object value, Record record) {
        Object result = value;
        for (List<String> path : paths) {
            String field = path.getLast();
            result = TreePaths.editObjects(result, path.subList(0, path.size() - 1), false, TreePaths.Lists.MAP_OVER,
                    object -> TreePaths.without(object, field));
        }
        return result;
    }
}
