package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.KafkaClientRequestHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.jspecify.annotations.Nullable;

import java.net.InetSocketAddress;

/// The client-facing Netty listener. Binds the server socket and owns the
/// [KafkaServerInitializer.DispatcherHolder], so connections are closed until a dispatcher is
/// installed - the listener is bound before the dispatcher exists (port-0 advertised
/// resolution), and the dispatcher is only installed after the initial metadata fetch.
///
/// Clients connect over the listener's security protocol: with TLS on `SSL` and `SASL_SSL`, and
/// required to log in with SASL on `SASL_PLAINTEXT` and `SASL_SSL`.
public final class KafkaListener implements AutoCloseable {

    private final EventLoopGroup parentGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup childGroup = new NioEventLoopGroup();
    private final KafkaServerInitializer.DispatcherHolder dispatcherHolder =
            new KafkaServerInitializer.DispatcherHolder();
    private final GatewayMetrics metrics;
    private final KafkaBodyCodec codec;
    private final SecurityProtocol securityProtocol;
    private final @Nullable ListenerTls tls;

    private Channel serverChannel;

    /// A `SASL_PLAINTEXT` listener.
    public KafkaListener(GatewayMetrics metrics, KafkaBodyCodec codec) {
        this(metrics, codec, SecurityProtocol.SASL_PLAINTEXT, null);
    }

    /// @param tls the listener's TLS, required for `SSL` and `SASL_SSL` and `null` otherwise
    public KafkaListener(
            GatewayMetrics metrics,
            KafkaBodyCodec codec,
            SecurityProtocol securityProtocol,
            @Nullable ListenerTls tls
    ) {
        boolean usesTls = securityProtocol == SecurityProtocol.SSL || securityProtocol == SecurityProtocol.SASL_SSL;
        if (usesTls == (tls == null)) {
            throw new IllegalArgumentException(securityProtocol + " listener " + (usesTls ? "needs" : "takes no") + " TLS");
        }
        this.metrics = metrics;
        this.codec = codec;
        this.securityProtocol = securityProtocol;
        this.tls = tls;
    }

    /// Binds the server socket and returns the bound port (useful when `port` is 0).
    public int bind(String host, int port) throws InterruptedException {
        var server = new ServerBootstrap();
        server.group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new KafkaServerInitializer(dispatcherHolder, metrics, codec, securityProtocol, tls));
        serverChannel = server.bind(host, port).sync().channel();
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /// Installs the request dispatcher; connections accepted before this point are closed.
    public void installDispatcher(KafkaClientRequestHandler dispatcher) {
        dispatcherHolder.set(dispatcher);
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        parentGroup.shutdownGracefully();
        childGroup.shutdownGracefully();
    }
}
