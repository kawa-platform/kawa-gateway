package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Filter that keeps only records with a `header` record header whose value starts with `value`.
///
/// @param header      header key to compare
/// @param value       prefix the header value must start with
/// @param errorHeader see [VirtualTopicFilterConfig#errorHeader()]; `true` when omitted
public record HeaderStartsWithFilterConfig(
        String header,
        String value,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements VirtualTopicFilterConfig {

    public HeaderStartsWithFilterConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
    }

    public HeaderStartsWithFilterConfig(String header, String value) {
        this(header, value, true);
    }
}
