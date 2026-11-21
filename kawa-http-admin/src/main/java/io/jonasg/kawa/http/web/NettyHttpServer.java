package io.jonasg.kawa.http.web;

import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jspecify.annotations.Nullable;

import java.net.InetSocketAddress;

/// Netty HTTP listener that serves a [Router]: HTTP decoding, the optional CORS handler and the
/// [HttpRouterHandler] dispatcher, on threads of its own. The admin server and the health server
/// both run on it.
public final class NettyHttpServer {

    private final String host;
    private final int port;
    private final Router router;
    private final @Nullable CorsConfig cors;
    private final AuthenticationProvider authenticationProvider;
    private final int routerExecutorThreads;

    private @Nullable EventLoopGroup bossGroup;
    private @Nullable EventLoopGroup workerGroup;
    private @Nullable EventExecutorGroup routerExecutorGroup;
    private @Nullable Channel serverChannel;

    /// @param authenticationProvider decides the requests to non-public routes; public routes never consult it
    public NettyHttpServer(
            String host,
            int port,
            Router router,
            @Nullable CorsConfig cors,
            AuthenticationProvider authenticationProvider
    ) {
        this.host = host;
        this.port = port;
        this.router = router;
        this.cors = cors;
        this.authenticationProvider = authenticationProvider;
        this.routerExecutorThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        routerExecutorGroup = new DefaultEventExecutorGroup(routerExecutorThreads);
        var bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("httpCodec", new HttpServerCodec());
                        ch.pipeline().addLast("httpAggregator", new HttpObjectAggregator(65536));
                        if (cors != null) {
                            ch.pipeline().addLast("cors", new CorsHandler(CorsConfigFactory.from(cors)));
                        }
                        ch.pipeline().addLast(routerExecutorGroup, "httpRouter", new HttpRouterHandler(router, authenticationProvider));
                    }
                });
        serverChannel = bootstrap.bind(host, port).sync().channel();
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (routerExecutorGroup != null) {
            routerExecutorGroup.shutdownGracefully();
        }
    }

    /// The port the server is bound to. Only after [#start].
    @SuppressWarnings("ConstantConditions")
    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public boolean hasDedicatedRouterExecutor() {
        return routerExecutorGroup != null;
    }
}
