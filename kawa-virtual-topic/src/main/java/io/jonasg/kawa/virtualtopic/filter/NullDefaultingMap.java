package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.NullValue;

import java.util.LinkedHashMap;

/// A decoded object whose absent fields read as `null` rather than raising a CEL "no such key"
/// error, so `value.status == "PAID"` is `false` for a document without `status`. Presence can
/// still be tested with `has(value.status)`. Keeps the document's field order.
final class NullDefaultingMap extends LinkedHashMap<String, Object> {

    @Override
    public Object get(Object key) {
        return containsKey(key) ? super.get(key) : NullValue.NULL_VALUE;
    }
}
