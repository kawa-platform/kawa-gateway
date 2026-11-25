package io.jonasg.kawa.mcp.peekrecords;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/// How `peek_records` reads record values: detected per record ([#AUTO]) or as one format the caller names.
enum PayloadFormat {
    AUTO,
    JSON,
    XML,
    /// Avro in the schema registry wire format, decoded with the schema of the registry id it carries.
    AVRO,
    /// UTF-8 text.
    STRING,
    /// Not decoded: the bytes as Base64.
    RAW;

    /// The name the tool's `format` argument and its results use.
    String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    static List<String> labels() {
        return Arrays.stream(values()).map(PayloadFormat::label).toList();
    }
}
