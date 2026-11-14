package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.DefaultFieldTransformationConfig;
import io.jonasg.kawa.config.FieldPath;
import org.apache.kafka.common.record.internal.Record;

import java.util.List;

/// Sets a field that is absent or `null`, creating missing parent objects; a list on the path
/// sets it in every element. The default is converted into the value tree once, when compiled.
final class DefaultFieldMapping implements ValueMapping {

    private final List<String> parent;
    private final String field;
    private final Object defaultValue;

    DefaultFieldMapping(DefaultFieldTransformationConfig config) {
        List<String> path = FieldPath.parse(config.field());
        this.parent = path.subList(0, path.size() - 1);
        this.field = path.getLast();
        try {
            this.defaultValue = ValueTree.of(config.value());
        } catch (PayloadMappingException e) {
            throw new IllegalArgumentException("invalid default for '" + config.field() + "': " + e.getMessage(), e);
        }
    }

    @Override
    public Object apply(Object value, Record record) {
        return TreePaths.editObjects(value, parent, true, TreePaths.Lists.MAP_OVER,
                object -> TreePaths.present(object, field) ? object : TreePaths.with(object, field, defaultValue));
    }
}
