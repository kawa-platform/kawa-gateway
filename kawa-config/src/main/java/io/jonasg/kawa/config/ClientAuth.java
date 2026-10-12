package io.jonasg.kawa.config;

/// Whether a TLS listener asks connecting clients for a certificate, as Kafka's
/// `ssl.client.auth` names it.
public enum ClientAuth {
    /// Clients are not asked for a certificate.
    NONE,
    /// Clients are asked for a certificate but may connect without one.
    REQUESTED,
    /// Clients must present a certificate the listener's truststore trusts.
    REQUIRED
}
