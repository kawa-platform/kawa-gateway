package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

/// XML-encoded payload. Filters see the document's root content as a value tree: elements as
/// fields, attributes as `@`-prefixed fields, and all text as strings.
///
/// `root` is left out of the JSON while unset.
///
/// @param onDecodeError what to do with a record that isn't decodable XML - [DecodeErrorPolicy#SKIP] by default
/// @param root          the root element name; required when transformed values are encoded in this format,
///                      and when set, decoding refuses a document with another root
@JsonInclude(JsonInclude.Include.NON_NULL)
public record XmlFormatConfig(DecodeErrorPolicy onDecodeError, @Nullable String root) implements PayloadFormatConfig {

    public XmlFormatConfig {
        onDecodeError = onDecodeError == null ? DecodeErrorPolicy.SKIP : onDecodeError;
        if (root != null && root.isBlank()) {
            throw new IllegalArgumentException("xml 'root' must not be blank");
        }
    }

    public XmlFormatConfig(DecodeErrorPolicy onDecodeError) {
        this(onDecodeError, null);
    }

    public XmlFormatConfig() {
        this(DecodeErrorPolicy.SKIP);
    }
}
