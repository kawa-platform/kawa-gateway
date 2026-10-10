package io.jonasg.kawa.server.endpoint;

import io.jonasg.kawa.core.ApiTransform;

/// Broker-endpoint transform: points the broker or coordinator endpoints in one API's response at
/// the gateway's advertised listener.
///
/// @param <Req>  the decoded request body type
/// @param <Resp> the decoded response body type
public interface BrokerEndpointTransform<Req, Resp> extends ApiTransform<Req, Resp> {
}
