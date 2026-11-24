package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Filter that keeps only records with a `header` record header whose value contains `value`.
///
/// @param header      header key to compare
/// @param value       substring the header value must contain
/// @param errorHeader see [VirtualTopicFilterConfig#errorHeader()]; `true` when omitted
public record HeaderContainsFilterConfig(
        String header,
        String value,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements VirtualTopicFilterConfig {

    public HeaderContainsFilterConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
    }

    public HeaderContainsFilterConfig(String header, String value) {
        this(header, value, true);
    }
}
