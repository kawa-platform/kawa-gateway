package io.jonasg.kawa.config;

/// Encodes the transformed value in another format than the virtual topic's `valueFormat`.
/// Only allowed as the last step of a virtual topic's transformations.
///
/// @param format the format the value is encoded in
public record ConvertTransformationConfig(PayloadFormatConfig format) implements TransformationConfig {

    public ConvertTransformationConfig {
        if (format == null) {
            throw new IllegalArgumentException("convert transformation requires a 'format'");
        }
    }
}
