package io.jonasg.kawa.virtualtopic.mapping;

import org.apache.kafka.common.header.Header;

/// Maps the headers of a record whose value a virtual topic's transformations rewrote, so they
/// keep describing the delivered value (see [HeaderMappings]).
///
/// Implementations never modify `headers` or the headers in it, and are thread-safe.
@FunctionalInterface
public interface HeaderMapping {

    /// Passes the headers through unchanged.
    HeaderMapping IDENTITY = headers -> headers;

    Header[] apply(Header[] headers);
}
