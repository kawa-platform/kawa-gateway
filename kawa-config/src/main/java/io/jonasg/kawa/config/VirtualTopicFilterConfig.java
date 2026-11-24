package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/// Virtual-topic consume filter configuration.
///
/// The `type` discriminator selects the concrete filter config, each of which defines
/// its own fields. New filter kinds are added by implementing this interface and registering a
/// [JsonSubTypes.Type] entry below.
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = HeaderEqualsFilterConfig.class, name = "headerEquals"),
        @JsonSubTypes.Type(value = HeaderContainsFilterConfig.class, name = "headerContains"),
        @JsonSubTypes.Type(value = HeaderStartsWithFilterConfig.class, name = "headerStartsWith"),
        @JsonSubTypes.Type(value = HeaderMatchesFilterConfig.class, name = "headerMatches"),
        @JsonSubTypes.Type(value = CelFilterConfig.class, name = "cel")
})
public sealed interface VirtualTopicFilterConfig
        permits HeaderEqualsFilterConfig, HeaderContainsFilterConfig, HeaderStartsWithFilterConfig,
        HeaderMatchesFilterConfig, CelFilterConfig {

    /// Whether a record this filter cannot decode, delivered anyway because the value format's
    /// `onDecodeError` is `include`, carries the `kawa-error` headers that say why. Never `null`:
    /// `true` when the config omits it.
    Boolean errorHeader();
}
