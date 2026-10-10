package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Response;

import java.util.List;

/// Points every broker and coordinator endpoint in a response at the gateway's advertised
/// listener, so clients keep talking to the gateway instead of connecting to the brokers directly.
/// Dispatches to one [BrokerEndpointTransform] per API.
///
/// Gateway plumbing rather than a feature: it always runs, whatever else is configured.
public final class BrokerEndpointInterceptor implements Interceptor {

    private final BrokerEndpointTransformRegistry registry;

    public BrokerEndpointInterceptor(AdvertisedListener advertised) {
        this.registry = new BrokerEndpointTransformRegistry(List.of(
                new MetadataBrokerEndpointTransform(advertised),
                new FindCoordinatorBrokerEndpointTransform(advertised),
                new DescribeClusterBrokerEndpointTransform(advertised)));
    }

    @Override
    public boolean appliesToRequest(Request request) {
        return false;
    }

    @Override
    public boolean appliesToResponse(Response response) {
        return registry.hasApiKey((short) response.apiKey());
    }

    @Override
    public void onResponse(
            GatewayContext context,
            Response response
    ) {
        registry.onResponse(context, (short) response.apiKey(), response.body());
    }
}
