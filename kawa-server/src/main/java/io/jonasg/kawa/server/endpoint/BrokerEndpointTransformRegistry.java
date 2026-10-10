package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.core.GatewayContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/// Registry for broker-endpoint transforms. Owns the single unchecked cast (`Object → Resp`) so
/// that transform implementations stay cast-free.
public final class BrokerEndpointTransformRegistry {

    private final Map<Short, BrokerEndpointTransform<?, ?>> transforms;

    public BrokerEndpointTransformRegistry(Collection<? extends BrokerEndpointTransform<?, ?>> transforms) {
        Map<Short, BrokerEndpointTransform<?, ?>> byKey = new HashMap<>();
        for (BrokerEndpointTransform<?, ?> transform : transforms) {
            if (byKey.put(transform.apiKey(), transform) != null) {
                throw new IllegalArgumentException(
                        "Duplicate broker-endpoint transform for api key " + transform.apiKey());
            }
        }
        this.transforms = Map.copyOf(byKey);
    }

    @SuppressWarnings("unchecked")
    public void onResponse(GatewayContext context, short apiKey, Object body) {
        if (body == null) {
            return;
        }
        BrokerEndpointTransform<Object, Object> transform =
                (BrokerEndpointTransform<Object, Object>) transforms.get(apiKey);
        if (transform != null) {
            transform.onResponse(context, body);
        }
    }

    public boolean hasApiKey(short apiKey) {
        return transforms.containsKey(apiKey);
    }
}
