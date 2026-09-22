package io.jonasg.kawa.server.auth;

import org.apache.kafka.common.message.SaslAuthenticateResponseData;

import java.util.function.Consumer;

/// Outcome of a `SaslAuthenticate` credential check. A [Success] carries the authenticated
/// username alongside the wire response; a [Failure] carries only the error response; a
/// [Pending] carries a challenge response for a multi-round-trip mechanism (SCRAM) whose
/// exchange is not finished yet.
public sealed interface AuthenticationResult {

    SaslAuthenticateResponseData response();

    @SuppressWarnings("UnusedReturnValue")
    AuthenticationResult onSuccess(Consumer<String> successHandler);

    /// Authentication succeeded. `username` is the authenticated kawa user.
    record Success(String username, SaslAuthenticateResponseData response) implements AuthenticationResult {
        @Override
        public AuthenticationResult onSuccess(Consumer<String> successHandler) {
            successHandler.accept(username);
            return this;
        }
    }

    /// Authentication failed (unknown user, wrong password, or malformed payload).
    record Failure(SaslAuthenticateResponseData response) implements AuthenticationResult {
        @Override
        public AuthenticationResult onSuccess(Consumer<String> successHandler) {
            return this;
        }
    }

    /// The exchange is still open: the response carries the next challenge for the client.
    record Pending(SaslAuthenticateResponseData response) implements AuthenticationResult {
        @Override
        public AuthenticationResult onSuccess(Consumer<String> successHandler) {
            return this;
        }
    }
}
