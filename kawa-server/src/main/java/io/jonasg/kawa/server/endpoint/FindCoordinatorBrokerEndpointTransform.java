package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.FindCoordinatorResponseData;

/// Points the group and transaction coordinators in a FindCoordinator response at the advertised
/// listener: the single coordinator of v0-v3 and every entry of `coordinators` (v4+).
public final class FindCoordinatorBrokerEndpointTransform
        implements BrokerEndpointTransform<Object, FindCoordinatorResponseData> {

    private final AdvertisedListener advertised;

    public FindCoordinatorBrokerEndpointTransform(AdvertisedListener advertised) {
        this.advertised = advertised;
    }

    @Override
    public short apiKey() {
        return KafkaApiRegistry.FIND_COORDINATOR;
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
            FindCoordinatorResponseData data
    ) {
        data.setHost(advertised.host());
        data.setPort(advertised.port());
        for (FindCoordinatorResponseData.Coordinator coordinator : data.coordinators()) {
            coordinator.setHost(advertised.host());
            coordinator.setPort(advertised.port());
        }
    }
}
