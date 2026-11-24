package io.jonasg.kawa.config;

import java.util.List;

/// Removes headers from a transformed record, typically ones that describe the stored format
/// (`content-type`, `__TypeId__`) after a [ConvertTransformationConfig] changed it.
///
/// Applied only to records whose value was transformed: a record delivered as stored keeps its
/// stored headers, so headers always describe the delivered bytes.
///
/// @param headers the names of the headers to remove, matched exactly (Kafka header names are
///                case-sensitive); every header with one of these names is removed, and a name the
///                record does not have is ignored
public record DropHeadersTransformationConfig(List<String> headers) implements TransformationConfig {

    public DropHeadersTransformationConfig {
        if (headers == null || headers.isEmpty()) {
            throw new IllegalArgumentException("dropHeaders transformation requires at least one header");
        }
        headers = List.copyOf(headers);
        if (headers.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("dropHeaders transformation: header names must not be blank");
        }
    }
}
