package io.jonasg.kawa.virtualtopic.filter;

import java.nio.ByteBuffer;

/// Decodes a record payload into a generic value tree that content-based filters evaluate:
/// `Map<String, Object>` for objects, `List<Object>` for arrays, and `String`, `Long`,
/// `Double`, `Boolean` or CEL's `NullValue` for scalars.
///
/// One implementation per [io.jonasg.kawa.config.PayloadFormatConfig], so filters never need to
/// know the wire format. Implementations must be thread-safe.
public interface PayloadDecoder {

    /// @throws PayloadDecodeException if `payload` is null, empty or not valid in this format
    Object decode(ByteBuffer payload);
}
