package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Filter that keeps only records whose `header` record header equals `value`.
///
/// @param header      header key to compare
/// @param value       expected header value
/// @param errorHeader see [VirtualTopicFilterConfig#errorHeader()]; `true` when omitted
public record HeaderEqualsFilterConfig(
        String header,
        String value,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements VirtualTopicFilterConfig {

    public HeaderEqualsFilterConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
    }

    public HeaderEqualsFilterConfig(String header, String value) {
        this(header, value, true);
    }
}
