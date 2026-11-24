package io.jonasg.kawa.config;

/// Sets a header on a transformed record, typically one that describes the format the
/// transformations encode in (`content-type: application/json`).
///
/// Applied only to records whose value was transformed: a record delivered as stored keeps its
/// stored headers, so headers always describe the delivered bytes.
///
/// @param header the header name, matched exactly (Kafka header names are case-sensitive); every
///               header the record already has with this name is replaced by this one
/// @param value  the header value, encoded as UTF-8
public record SetHeaderTransformationConfig(String header, String value) implements TransformationConfig {

    public SetHeaderTransformationConfig {
        if (header == null || header.isBlank()) {
            throw new IllegalArgumentException("setHeader transformation requires a 'header'");
        }
        if (value == null) {
            throw new IllegalArgumentException("setHeader transformation requires a 'value'");
        }
    }
}
