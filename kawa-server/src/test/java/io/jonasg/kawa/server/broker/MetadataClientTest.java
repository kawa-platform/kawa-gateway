package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class MetadataClientTest {

    @Test
    void connectWithRetryWaitsForBrokerToComeUp() throws Exception {
        // given — a free port that is not yet listening
        int port;
        try (var probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        var group = new NioEventLoopGroup(1);
        var client = new MetadataClient(bootstrapAt(port), group, null, new MetadataCache(), null, null);

        // when — the listener comes up after the first connect is refused
        var serverThread = new Thread(() -> {
            try {
                Thread.sleep(500);
                try (var server = new ServerSocket(port)) {
                    Thread.sleep(2000);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        serverThread.start();

        Channel channel = client.connectWithRetry().channel();

        // then
        assertThat(channel.isActive())
                .withFailMessage(() -> "Metadata client did not connect once the broker came up on port " + port)
                .isTrue();
        channel.close().sync();
        serverThread.join();
        group.shutdownGracefully().sync();
    }

    @Test
    void refreshFetchesTheMetadataOnDemand() throws Exception {
        // given - a started client and a broker that creates 'orders' afterwards
        var group = new NioEventLoopGroup(1);
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        Set<String> brokerTopics = ConcurrentHashMap.newKeySet();
        Channel broker = startBroker(group, codec, brokerTopics, Integer.MAX_VALUE);
        int port = ((InetSocketAddress) broker.localAddress()).getPort();
        var cache = new MetadataCache();
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var pool = new BrokerClientPool(group, codec, new InterceptorPipeline(List.of()), metrics, cache, "127.0.0.1", port);
        var client = new MetadataClient(bootstrapAt(port), group, codec, cache, pool, metrics);

        try {
            client.start();
            brokerTopics.add("orders");

            // when
            client.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

            // then - the cache holds the topic without waiting for the scheduled refresh
            assertThat(cache.contains("orders"))
                    .withFailMessage(() -> "Topic 'orders' is not in the metadata cache after an on-demand refresh")
                    .isTrue();
        } finally {
            client.stop();
            broker.close().sync();
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void refreshFailsWhenTheConnectionClosesWhileTheRequestIsInFlight() throws Exception {
        // given - a broker that answers the initial Metadata request and closes the connection on the next one
        var group = new NioEventLoopGroup(1);
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        Channel broker = startBroker(group, codec, Set.of(), 1);
        int port = ((InetSocketAddress) broker.localAddress()).getPort();
        var cache = new MetadataCache();
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var pool = new BrokerClientPool(group, codec, new InterceptorPipeline(List.of()), metrics, cache, "127.0.0.1", port);
        var client = new MetadataClient(bootstrapAt(port), group, codec, cache, pool, metrics);

        try {
            client.start();

            // when
            var refresh = client.refresh().toCompletableFuture();

            // then - the refresh fails as soon as the connection closes instead of never completing
            assertThatThrownBy(() -> refresh.get(2, TimeUnit.SECONDS))
                    .withFailMessage(() -> "refresh() did not fail with an IllegalStateException when the connection closed")
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
        } finally {
            client.stop();
            broker.close().sync();
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void refreshSucceedsAgainAfterTheBrokerDropsTheConnection() throws Exception {
        // given - a broker that answers two Metadata requests per connection, then closes it
        var group = new NioEventLoopGroup(1);
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        Channel broker = startBroker(group, codec, Set.of(), 2);
        int port = ((InetSocketAddress) broker.localAddress()).getPort();
        var cache = new MetadataCache();
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var pool = new BrokerClientPool(group, codec, new InterceptorPipeline(List.of()), metrics, cache, "127.0.0.1", port);
        var client = new MetadataClient(bootstrapAt(port), group, codec, cache, pool, metrics);

        try {
            client.start();
            client.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

            // when - the broker drops the connection on the next request
            var dropped = client.refresh().toCompletableFuture();
            assertThatThrownBy(() -> dropped.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);

            // then - the client reconnects on its own and refreshes again
            await().atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(refreshSucceeds(client))
                            .withFailMessage(() -> "Metadata client did not refresh again after the broker dropped its connection")
                            .isTrue());
        } finally {
            client.stop();
            broker.close().sync();
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void startsFromTheNextBootstrapServerWhenTheFirstIsDown() throws Exception {
        // given - a first bootstrap server that refuses connections, and a live broker behind the second
        int deadPort;
        try (var probe = new ServerSocket(0)) {
            deadPort = probe.getLocalPort();
        }
        var group = new NioEventLoopGroup(1);
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        Channel broker = startBroker(group, codec, Set.of("orders"), Integer.MAX_VALUE);
        int port = ((InetSocketAddress) broker.localAddress()).getPort();
        var cache = new MetadataCache();
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var pool = new BrokerClientPool(group, codec, new InterceptorPipeline(List.of()), metrics, cache, "127.0.0.1", port);
        var client = new MetadataClient(List.of(unresolved(deadPort), unresolved(port)), group, codec, cache, pool, metrics);

        try {
            // when
            client.start();

            // then - the client is up and sees the topics of the broker behind the second bootstrap server
            assertThat(cache.contains("orders"))
                    .withFailMessage(() -> "Metadata client did not start from the second bootstrap server on port " + port)
                    .isTrue();
        } finally {
            client.stop();
            broker.close().sync();
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void reconnectsThroughTheBrokerItLearnedWhenTheBootstrapServerGoesDown() throws Exception {
        // given - a bootstrap broker whose metadata names a second broker, which is also up
        var group = new NioEventLoopGroup(1);
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        Channel second = startBroker(group, codec, Set.of("orders"), Integer.MAX_VALUE, 0, new DefaultChannelGroup(GlobalEventExecutor.INSTANCE));
        int secondPort = ((InetSocketAddress) second.localAddress()).getPort();
        var firstConnections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        Channel first = startBroker(group, codec, Set.of("orders"), Integer.MAX_VALUE, secondPort, firstConnections);
        int firstPort = ((InetSocketAddress) first.localAddress()).getPort();
        var cache = new MetadataCache();
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var pool = new BrokerClientPool(group, codec, new InterceptorPipeline(List.of()), metrics, cache, "127.0.0.1", firstPort);
        var client = new MetadataClient(bootstrapAt(firstPort), group, codec, cache, pool, metrics);

        try {
            client.start();

            // when - the bootstrap broker goes away for good
            first.close().sync();
            firstConnections.close().awaitUninterruptibly();

            // then - the client gets back in through the second broker
            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(refreshSucceeds(client))
                            .withFailMessage(() -> "Metadata client did not reconnect through the broker it learned on port " + secondPort)
                            .isTrue());
        } finally {
            client.stop();
            second.close().sync();
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void refreshFailsWhenTheMetadataConnectionIsNotUp() {
        // given - a client that was never started
        var client = new MetadataClient(bootstrapAt(9092), null, null, new MetadataCache(), null, null);

        // when / then
        assertThatThrownBy(() -> client.refresh().toCompletableFuture().get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private static List<InetSocketAddress> bootstrapAt(int port) {
        return List.of(unresolved(port));
    }

    private static InetSocketAddress unresolved(int port) {
        return InetSocketAddress.createUnresolved("127.0.0.1", port);
    }

    private static boolean refreshSucceeds(MetadataClient client) {
        try {
            client.refresh().toCompletableFuture().get(1, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /// A broker answering ApiVersions and the first `metadataAnswers` Metadata requests of each
    /// connection, closing the connection without answering on the next one. The Metadata response
    /// lists `topics`, each with one partition led by broker 1.
    private static Channel startBroker(NioEventLoopGroup group, KafkaBodyCodec codec, Set<String> topics, int metadataAnswers)
            throws InterruptedException {
        return startBroker(group, codec, topics, metadataAnswers, 0, new DefaultChannelGroup(GlobalEventExecutor.INSTANCE));
    }

    /// As [#startBroker(NioEventLoopGroup, KafkaBodyCodec, Set, int)], but the Metadata response names
    /// broker 1 on `advertisedPort` (`0` for this broker's own port), and every accepted connection is
    /// added to `connections`, so a test can drop them all at once.
    private static Channel startBroker(NioEventLoopGroup group, KafkaBodyCodec codec, Set<String> topics, int metadataAnswers,
                                       int advertisedPort, ChannelGroup connections) throws InterruptedException {
        var responseHeaderCodec = new ResponseHeaderCodec();
        return new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        connections.add(ch);
                        ch.pipeline().addLast(new KafkaFrameDecoder());
                        ch.pipeline().addLast(new KafkaFrameEncoder());
                        var metadataRequests = new AtomicInteger();
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                ByteBuf request = (ByteBuf) msg;
                                short apiKey = request.getShort(request.readerIndex());
                                short apiVersion = request.getShort(request.readerIndex() + 2);
                                int correlationId = request.getInt(request.readerIndex() + 4);
                                request.release();
                                if (apiKey == ApiKeys.METADATA.id && metadataRequests.incrementAndGet() > metadataAnswers) {
                                    ctx.close();
                                    return;
                                }
                                int ownPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
                                Object body = apiKey == ApiKeys.METADATA.id
                                        ? metadata(topics, advertisedPort == 0 ? ownPort : advertisedPort)
                                        : new ApiVersionsResponseData();
                                short headerVersion = ApiKeys.forId(apiKey).responseHeaderVersion(apiVersion);
                                ByteBuf out = ctx.alloc().buffer();
                                responseHeaderCodec.encode(out, headerVersion, correlationId);
                                codec.encodeRequest(apiVersion, body, out);
                                ctx.writeAndFlush(out);
                            }
                        });
                    }
                })
                .bind("127.0.0.1", 0).sync().channel();
    }

    private static MetadataResponseData metadata(Set<String> topics, int port) {
        var response = new MetadataResponseData().setClusterId("test-cluster");
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("127.0.0.1").setPort(port));
        topics.forEach(name -> {
            var topic = new MetadataResponseData.MetadataResponseTopic().setName(name);
            topic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                    .setPartitionIndex(0).setLeaderId(1).setReplicaNodes(List.of(1)).setIsrNodes(List.of(1)));
            response.topics().add(topic);
        });
        return response;
    }
}
