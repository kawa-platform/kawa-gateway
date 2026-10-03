package io.jonasg.kawa.core;

import java.util.concurrent.CompletionStage;

/// A gateway interceptor. Interceptors observe and rewrite requests in the client-to-broker
/// direction ([#onRequest]) and responses in the broker-to-client direction
/// ([#onResponse]).
///
/// The core invokes interceptors in registration order and knows nothing about concrete
/// implementations (virtual topics, auth, metrics, etc.).
public interface Interceptor {

    default boolean appliesToRequest(Request request) {
        return true;
    }

    default boolean appliesToResponse(Response response) {
        return true;
    }

    /// Starts any asynchronous lookup [#onRequest] needs, e.g. reading a resource's current
    /// state from the broker. Called before the pipeline runs, for every interceptor whose
    /// [#appliesToRequest] holds; the pipeline then runs once every returned stage has
    /// completed, back on the connection's event loop. Must not block.
    ///
    /// Store the result in [GatewayContext#state] for [#onRequest] to read. A stage that
    /// completes exceptionally does not stop the request: [#onRequest] still runs and decides
    /// what a missing result means. Responses keep their order, but the broker may receive a
    /// later request from the same connection first.
    ///
    /// @return the lookup to wait for, or `null` when there is nothing to wait for (the default)
    default CompletionStage<?> prepare(
            GatewayContext context,
            Request request
    ) {
        return null;
    }

    default void onRequest(
            GatewayContext context,
            Request request
    ) {
        // no-op
    }

    default void onResponse(
            GatewayContext context,
            Response response
    ) {
        // no-op
    }
}
