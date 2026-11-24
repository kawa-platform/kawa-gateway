package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/// One step of a virtual topic's value transformation pipeline.
///
/// A virtual topic's `transformations` are applied in order, on fetch only. Value steps rewrite
/// the record value decoded with the topic's `valueFormat`, and an optional
/// [ConvertTransformationConfig] after them picks the format the result is encoded in (the
/// `valueFormat` otherwise). Header steps ([DropHeadersTransformationConfig],
/// [SetHeaderTransformationConfig]) rewrite the headers of the records a topic delivers, so they
/// describe the format delivered; they may come before or after `convert`. Keys are
/// never changed.
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
        @JsonSubTypes.Type(value = DefaultFieldTransformationConfig.class, name = "defaultField"),
        @JsonSubTypes.Type(value = CelTransformationConfig.class, name = "cel"),
        @JsonSubTypes.Type(value = ConvertTransformationConfig.class, name = "convert"),
        @JsonSubTypes.Type(value = DropHeadersTransformationConfig.class, name = "dropHeaders"),
        @JsonSubTypes.Type(value = SetHeaderTransformationConfig.class, name = "setHeader")
})
public sealed interface TransformationConfig
        permits DropFieldsTransformationConfig, RenameFieldTransformationConfig, DefaultFieldTransformationConfig,
        CelTransformationConfig, ConvertTransformationConfig, DropHeadersTransformationConfig,
        SetHeaderTransformationConfig {

    /// Whether a record this step fails on, delivered untransformed because the value format's
    /// `onDecodeError` is `include`, carries the `kawa-error` headers that say why. A value that
    /// does not decode counts as a failure of the first value step, one that does not encode as a
    /// failure of the `convert` step (the last value step without one). Header steps never fail, so
    /// their setting has no effect. Never `null`: `true` when the config omits it.
    Boolean errorHeader();
}
