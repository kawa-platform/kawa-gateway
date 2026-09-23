package io.jonasg.kawa.config;

/// JSON-encoded payload. Filters see the parsed document instead of the raw string.
///
/// @param onDecodeError what to do with a record that isn't valid JSON - [DecodeErrorPolicy#SKIP] by default
public record JsonFormatConfig(DecodeErrorPolicy onDecodeError) implements PayloadFormatConfig {

    public JsonFormatConfig {
        onDecodeError = onDecodeError == null ? DecodeErrorPolicy.SKIP : onDecodeError;
    }

    public JsonFormatConfig() {
        this(DecodeErrorPolicy.SKIP);
    }
}
