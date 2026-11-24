package io.jonasg.kawa.config;

import java.util.List;

/// Removes headers, for example ones that describe the stored format (`content-type`, `__TypeId__`)
/// after a [ConvertTransformationConfig] changed it.
///
/// Like every header step, it runs on every record with a value, and a topic with only header
/// steps never decodes its values. The one exception is a value delivered as stored after a failed
/// transformation while the topic has a `convert`: that value is still in the stored format, so its
/// headers stay. A tombstone keeps its headers too.
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
