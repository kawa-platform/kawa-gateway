package io.jonasg.kawa.server.broker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

final class MetadataResponseHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MetadataResponseHandler.class);

    private final MetadataClient client;
    private final Map<Integer, Consumer<ByteBuf>> pending;

    MetadataResponseHandler(MetadataClient client, Map<Integer, Consumer<ByteBuf>> pending) {
        this.client = client;
        this.pending = pending;
    }

    @Override
    public void channelRead(
            ChannelHandlerContext ctx,
            Object msg
    ) {
        ByteBuf buf = (ByteBuf) msg;
        log.debug("Metadata channel read {} bytes", buf.readableBytes());
        client.handleFrame(pending, buf);
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        log.warn("Metadata connection error", cause);
        ctx.close();
    }
}
