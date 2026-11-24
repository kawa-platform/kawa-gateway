package io.jonasg.kawa.config;

/// Encodes the transformed value in another format than the virtual topic's `valueFormat`.
/// At most one per virtual topic, after every value step; header steps may follow it.
///
/// @param format the format the value is encoded in
public record ConvertTransformationConfig(PayloadFormatConfig format) implements TransformationConfig {

    public ConvertTransformationConfig {
        if (format == null) {
            throw new IllegalArgumentException("convert transformation requires a 'format'");
        }
    }
}
