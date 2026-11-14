package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/// One step of a virtual topic's value transformation pipeline.
///
/// A virtual topic's `transformations` are applied in order, on fetch only, to the record value
/// decoded with the topic's `valueFormat`; keys and headers are never changed. Field steps
/// rewrite the decoded value, and an optional trailing [ConvertTransformationConfig] picks the
/// format the result is encoded in (the `valueFormat` otherwise).
///
/// The `type` discriminator selects the concrete step. New steps are added by implementing this
/// interface and registering a [JsonSubTypes.Type] entry below.
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DropFieldsTransformationConfig.class, name = "dropFields"),
        @JsonSubTypes.Type(value = RenameFieldTransformationConfig.class, name = "renameField"),
        @JsonSubTypes.Type(value = CelTransformationConfig.class, name = "cel"),
        @JsonSubTypes.Type(value = ConvertTransformationConfig.class, name = "convert")
})
public sealed interface TransformationConfig
        permits DropFieldsTransformationConfig, RenameFieldTransformationConfig, CelTransformationConfig,
        ConvertTransformationConfig {
}
