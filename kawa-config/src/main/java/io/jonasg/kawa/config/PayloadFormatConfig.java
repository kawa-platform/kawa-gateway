package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/// How a virtual topic's record payload is encoded, so content-based filters can decode it.
///
/// The `type` discriminator selects the concrete format, each of which defines its own settings
/// (a schema registry for Avro, descriptors for Protobuf, ...). New formats are added by
/// implementing this interface and registering a [JsonSubTypes.Type] entry below.
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = JsonFormatConfig.class, name = "json")
})
public sealed interface PayloadFormatConfig permits JsonFormatConfig {

    /// What to do with a record whose payload cannot be decoded in this format.
    DecodeErrorPolicy onDecodeError();
}
