package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
    void triesTheBrokersItLastLearnedAfterTheBootstrapServers() {
        // given - a cache that learned broker 7 from the last metadata response
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(Map.of(), Map.of(7, BrokerNode.of(7, "kafka-7", 9092, null)), "cluster"));
        var client = new MetadataClient(bootstrapAt(9092), null, null, cache, null, null);

        // when
        var candidates = client.candidates();

        // then
        assertThat(candidates)
                .withFailMessage(() -> "Metadata client did not try the learned broker after the bootstrap servers: " + candidates)
                .containsExactly(new MetadataClient.Endpoint("127.0.0.1", 9092), new MetadataClient.Endpoint("kafka-7", 9092));
    }

    @Test
    void doublesTheReconnectDelayUpToThirtySeconds() {
        // when / then
        assertThat(MetadataClient.nextBackoffMillis(500))
                .withFailMessage(() -> "Reconnect delay did not double from 0.5 s to 1 s")
                .isEqualTo(1_000);
        assertThat(MetadataClient.nextBackoffMillis(16_000))
                .withFailMessage(() -> "Reconnect delay did not cap at 30 s")
                .isEqualTo(30_000);
        assertThat(MetadataClient.nextBackoffMillis(30_000))
                .withFailMessage(() -> "Reconnect delay grew past 30 s")
                .isEqualTo(30_000);
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
        var responseHeaderCodec = new ResponseHeaderCodec();
        return new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
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
                                Object body = apiKey == ApiKeys.METADATA.id
                                        ? metadata(topics, ((InetSocketAddress) ctx.channel().localAddress()).getPort())
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
