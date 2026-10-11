package io.jonasg.kawa.config;

/// The format of a TLS key or trust store file, as Kafka's `ssl.*.type` settings name them.
public enum StoreType {
    PKCS12,
    JKS,
    /// PEM text: CA certificates for a truststore; a private key followed by its certificate
    /// chain, in one file, for a keystore.
    PEM
}
