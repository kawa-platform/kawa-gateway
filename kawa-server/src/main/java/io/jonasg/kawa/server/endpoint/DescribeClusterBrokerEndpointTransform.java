package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.DescribeClusterResponseData;

/// Points the brokers in a DescribeCluster response at the advertised listener. A response that
/// describes controllers (`EndpointType` 2, v1+) is left alone: the gateway does not proxy them.
public final class DescribeClusterBrokerEndpointTransform
        implements BrokerEndpointTransform<Object, DescribeClusterResponseData> {

    /// DescribeCluster's `EndpointType` for broker endpoints; v0 always describes brokers.
    private static final byte BROKER_ENDPOINTS = 1;

    private final AdvertisedListener advertised;

    public DescribeClusterBrokerEndpointTransform(AdvertisedListener advertised) {
        this.advertised = advertised;
    }

    @Override
    public short apiKey() {
        return KafkaApiRegistry.DESCRIBE_CLUSTER;
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
            DescribeClusterResponseData data
    ) {
        if (data.endpointType() != BROKER_ENDPOINTS) {
            return;
        }
        for (DescribeClusterResponseData.DescribeClusterBroker broker : data.brokers()) {
            broker.setHost(advertised.host());
            broker.setPort(advertised.port());
        }
    }
}
