package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.regex.Pattern;

/// Filter that keeps only records with a `header` record header whose value fully matches the
/// `value` regular expression (anchored, like `Pattern#matches`).
///
/// The regex is validated at config load time so an invalid pattern fails fast.
///
/// @param header      header key to compare
/// @param value       regular expression the header value must fully match
/// @param errorHeader see [VirtualTopicFilterConfig#errorHeader()]; `true` when omitted
public record HeaderMatchesFilterConfig(
        String header,
        String value,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements VirtualTopicFilterConfig {

    public HeaderMatchesFilterConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        Pattern.compile(value);
    }

    public HeaderMatchesFilterConfig(String header, String value) {
        this(header, value, true);
    }
}
