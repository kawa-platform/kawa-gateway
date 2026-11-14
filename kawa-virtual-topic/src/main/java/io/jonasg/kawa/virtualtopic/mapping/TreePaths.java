package io.jonasg.kawa.virtualtopic.mapping;

import dev.cel.common.values.NullValue;
import io.jonasg.kawa.virtualtopic.filter.NullDefaultingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/// Copy-on-write edits of a value tree along a dotted field path. Only the objects and lists on
/// the path are copied; everything else is shared with the input, which is never modified.
final class TreePaths {

    private TreePaths() {
    }

    /// Whether lists on the path are mapped over, or refused.
    enum Lists { MAP_OVER, REFUSE }

    /// Applies `edit` to every object `path` leads to and returns the updated tree. `edit`
    /// returns its argument to leave an object unchanged, which keeps it (and its parents)
    /// uncopied.
    ///
    /// @param create create missing (or `null`) objects on the path and refuse a scalar in the
    ///               way; otherwise a missing object or a scalar leaves the tree unchanged
    static Object editObjects(Object node, List<String> path, boolean create, Lists lists,
                              UnaryOperator<Map<String, Object>> edit) {
        return edit(node, path, 0, create, lists, edit);
    }

    private static Object edit(Object node, List<String> path, int depth, boolean create, Lists lists,
                               UnaryOperator<Map<String, Object>> edit) {
        if (node instanceof List<?> list) {
            if (lists == Lists.REFUSE) {
                throw new PayloadMappingException("'" + String.join(".", path.subList(0, depth)) + "' is a list");
            }
            List<Object> edited = null;
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                Object result = edit(element, path, depth, create, lists, edit);
                if (result != element && edited == null) {
                    edited = new ArrayList<>(list);
                }
                if (edited != null) {
                    edited.set(i, result);
                }
            }
            return edited == null ? node : edited;
        }
        if (!(node instanceof Map<?, ?> rawMap)) {
            if (create) {
                throw new PayloadMappingException("'" + (depth == 0 ? "value" : String.join(".", path.subList(0, depth)))
                        + "' is not an object");
            }
            return node;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) rawMap;
        if (depth == path.size()) {
            return edit.apply(map);
        }
        String segment = path.get(depth);
        Object child = present(map, segment) ? map.get(segment) : null;
        if (child == null) {
            if (!create) {
                return node;
            }
            return with(map, segment, edit(new NullDefaultingMap(), path, depth + 1, create, lists, edit));
        }
        // Compared with the instance passed down: a view such as Avro's returns a new wrapper per get
        Object editedChild = edit(child, path, depth + 1, create, lists, edit);
        return editedChild == child ? node : with(map, segment, editedChild);
    }

    /// The value at `path` below `object`, or `null` when absent. Never crosses a list.
    static Object get(Map<String, Object> object, List<String> path) {
        Object node = object;
        for (int i = 0; i < path.size(); i++) {
            if (node instanceof List<?>) {
                throw new PayloadMappingException("'" + String.join(".", path.subList(0, i)) + "' is a list");
            }
            if (!(node instanceof Map<?, ?> map) || !map.containsKey(path.get(i))) {
                return null;
            }
            node = map.get(path.get(i));
        }
        return node;
    }

    /// Whether `object` has `key` with a value other than `null`.
    static boolean present(Map<String, Object> object, String key) {
        return object.containsKey(key) && !(object.get(key) instanceof NullValue);
    }

    /// A copy of `object` with `key` set to `value`, in place when the key exists.
    static Map<String, Object> with(Map<String, Object> object, String key, Object value) {
        Map<String, Object> copy = copy(object);
        copy.put(key, value);
        return copy;
    }

    /// A copy of `object` without `key`, or `object` itself when it has no such key.
    static Map<String, Object> without(Map<String, Object> object, String key) {
        if (!object.containsKey(key)) {
            return object;
        }
        Map<String, Object> copy = copy(object);
        copy.remove(key);
        return copy;
    }

    /// A copy of `object` with `from` renamed to `to` at the same position.
    static Map<String, Object> renamed(Map<String, Object> object, String from, String to) {
        Map<String, Object> copy = new NullDefaultingMap();
        object.forEach((key, value) -> {
            if (key.equals(from)) {
                copy.put(to, value);
            } else if (!key.equals(to)) {
                copy.put(key, value);
            }
        });
        return copy;
    }

    private static Map<String, Object> copy(Map<String, Object> object) {
        Map<String, Object> copy = new NullDefaultingMap();
        copy.putAll(object);
        return copy;
    }
}
