package io.jonasg.kawa.virtualtopic.mapping;

import io.jonasg.kawa.config.FieldPath;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import org.apache.kafka.common.record.internal.Record;

import java.util.List;
import java.util.Map;

/// Moves a field to another path, keeping its value. Within one object the field keeps its
/// position; moved to another object it is appended there, creating missing parents. A list is
/// mapped over only on the part `from` and `to` share (`lines.sku` to `lines.code` renames in
/// every line); below that part neither path may cross a list. An absent source is ignored.
final class RenameFieldMapping implements ValueMapping {

    private final List<String> shared;
    private final List<String> from;
    private final List<String> to;

    RenameFieldMapping(RenameFieldTransformationConfig config) {
        List<String> fromPath = FieldPath.parse(config.from());
        List<String> toPath = FieldPath.parse(config.to());
        int common = 0;
        while (common < Math.min(fromPath.size(), toPath.size()) - 1 && fromPath.get(common).equals(toPath.get(common))) {
            common++;
        }
        this.shared = fromPath.subList(0, common);
        this.from = fromPath.subList(common, fromPath.size());
        this.to = toPath.subList(common, toPath.size());
    }

    @Override
    public Object apply(Object value, Record record) {
        return TreePaths.editObjects(value, shared, false, TreePaths.Lists.MAP_OVER, this::move);
    }

    private Map<String, Object> move(Map<String, Object> object) {
        if (from.size() == 1 && to.size() == 1) {
            return object.containsKey(from.getFirst()) ? TreePaths.renamed(object, from.getFirst(), to.getFirst()) : object;
        }
        Object sourceParent = TreePaths.get(object, from.subList(0, from.size() - 1));
        if (sourceParent instanceof List<?>) {
            throw new PayloadMappingException("'" + String.join(".", from.subList(0, from.size() - 1)) + "' is a list");
        }
        if (!(sourceParent instanceof Map<?, ?> source) || !source.containsKey(from.getLast())) {
            return object;
        }
        Object moved = source.get(from.getLast());
        Object removed = TreePaths.editObjects(object, from.subList(0, from.size() - 1), false, TreePaths.Lists.REFUSE,
                parent -> TreePaths.without(parent, from.getLast()));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) TreePaths.editObjects(removed, to.subList(0, to.size() - 1), true,
                TreePaths.Lists.REFUSE, parent -> TreePaths.with(parent, to.getLast(), moved));
        return result;
    }
}
