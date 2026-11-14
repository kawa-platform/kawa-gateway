package io.jonasg.kawa.virtualtopic.filter;

/// Encodes a generic value tree into a record payload: the inverse of [PayloadDecoder].
///
/// The value tree is the one [PayloadDecoder] produces: `Map<String, Object>` for objects,
/// `List<Object>` for arrays, and `String`, `Long`, `Double`, `Boolean` or CEL's `NullValue` for
/// scalars. For every value `v` a decoder of the same format produced, `decode(encode(v))` equals
/// `v`; each implementation documents what does not survive the opposite direction.
///
/// One implementation per [io.jonasg.kawa.config.PayloadFormatConfig]. Implementations must be
/// thread-safe.
public interface PayloadEncoder {

    /// @throws PayloadEncodeException if `value` holds a type outside the value tree or a value
    ///                                this format cannot represent
    byte[] encode(Object value);
}
