package io.jonasg.kawa.server.broker;

import java.net.InetSocketAddress;

/// A host and port the gateway connects to: a bootstrap server, or a broker the cache knows.
record Endpoint(String host, int port) {

    /// The endpoint of a configured bootstrap address, keeping its host name as written.
    static Endpoint of(InetSocketAddress address) {
        return new Endpoint(address.getHostString(), address.getPort());
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
