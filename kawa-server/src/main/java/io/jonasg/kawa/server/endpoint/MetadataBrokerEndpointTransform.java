package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.MetadataResponseData;

/// Points the brokers in a Metadata response at the advertised listener, so clients send produce
/// and fetch requests for every partition leader to the gateway.
public final class MetadataBrokerEndpointTransform
        implements BrokerEndpointTransform<Object, MetadataResponseData> {

    private final AdvertisedListener advertised;

    public MetadataBrokerEndpointTransform(AdvertisedListener advertised) {
        this.advertised = advertised;
    }

    @Override
    public short apiKey() {
        return KafkaApiRegistry.METADATA;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            Object body
    ) {
        // no-op
    }

    @Override
    public void onResponse(
            GatewayContext context,
            MetadataResponseData data
    ) {
        for (MetadataResponseData.MetadataResponseBroker broker : data.brokers()) {
            broker.setHost(advertised.host());
            broker.setPort(advertised.port());
        }
    }
}
