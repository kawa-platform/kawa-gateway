package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Sets a header, typically one that describes the format the transformations encode in
/// (`content-type: application/json`).
///
/// Like every header step, it runs on every record with a value, and a topic with only header
/// steps never decodes its values. The one exception is a value delivered as stored after a failed
/// transformation while the topic has a `convert`: that value is still in the stored format, so its
/// headers stay. A tombstone keeps its headers too.
///
/// @param header      the header name, matched exactly (Kafka header names are case-sensitive); every
///                    header the record already has with this name is replaced by this one
/// @param value       the header value, encoded as UTF-8
/// @param errorHeader see [TransformationConfig#errorHeader()]; `true` when omitted
public record SetHeaderTransformationConfig(
        String header,
        String value,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements TransformationConfig {

    public SetHeaderTransformationConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        if (header == null || header.isBlank()) {
            throw new IllegalArgumentException("setHeader transformation requires a 'header'");
        }
        if (value == null) {
            throw new IllegalArgumentException("setHeader transformation requires a 'value'");
        }
    }

    public SetHeaderTransformationConfig(String header, String value) {
        this(header, value, true);
    }
}
