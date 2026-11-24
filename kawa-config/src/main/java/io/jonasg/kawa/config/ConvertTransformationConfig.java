package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Encodes the transformed value in another format than the virtual topic's `valueFormat`.
/// At most one per virtual topic, after every value step; header steps may follow it.
///
/// @param format the format the value is encoded in
/// @param errorHeader see [TransformationConfig#errorHeader()]; `true` when omitted
public record ConvertTransformationConfig(
        PayloadFormatConfig format,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements TransformationConfig {

    public ConvertTransformationConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        if (format == null) {
            throw new IllegalArgumentException("convert transformation requires a 'format'");
        }
    }

    public ConvertTransformationConfig(PayloadFormatConfig format) {
        this(format, true);
    }
}
