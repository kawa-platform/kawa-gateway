package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/// What a content-based filter does with a record whose payload cannot be decoded.
///
/// A schema registry outage (unreachable, timed out, HTTP 5xx) is not a record error, so no policy
/// applies to it: the fetched partition fails instead, and its records are fetched again once the
/// registry is back.
public enum DecodeErrorPolicy {

    /// Drop the record from the fetch response, as if it did not match.
    @JsonProperty("skip")
    SKIP,

    /// Deliver the record, as if it matched.
    @JsonProperty("include")
    INCLUDE,

    /// Fail the fetch.
    @JsonProperty("fail")
    FAIL
}
