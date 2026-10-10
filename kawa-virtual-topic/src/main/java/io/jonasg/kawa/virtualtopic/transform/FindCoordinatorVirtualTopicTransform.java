package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import org.apache.kafka.common.message.FindCoordinatorResponseData;

/// Rewrites the host and port of the FindCoordinatorResponse to point at the gateway's advertised listener.
/// This is necessary because the FindCoordinatorResponse is used by clients to connect to the coordinator,
/// and we want them to connect to the gateway instead of the broker directly.
public final class FindCoordinatorVirtualTopicTransform
        implements VirtualTopicTransform<Object, FindCoordinatorResponseData> {

    private final AdvertisedListener advertised;

    public FindCoordinatorVirtualTopicTransform(AdvertisedListener advertised) {
        this.advertised = advertised;
    }

    @Override
    public short apiKey() {
        return 10;
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
