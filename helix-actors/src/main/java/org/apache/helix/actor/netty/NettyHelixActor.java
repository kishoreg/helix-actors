package org.apache.helix.actor.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.helix.HelixManager;
import org.apache.helix.actor.api.HelixActor;
import org.apache.helix.actor.api.HelixActorCallback;
import org.apache.helix.actor.api.HelixActorMessageCodec;
import org.apache.helix.actor.api.HelixActorResolver;
import org.apache.helix.actor.api.HelixActorScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides partition/state-level messaging among nodes in a Helix cluster.
 *
 * <p>
 *     The message format is
 <pre>
     +----------------------+
     | totalLength (4B)     |
     +----------------------+
     | messageId (16B)      |
     +----------------------+
     | clusterLength (4B)   |
     +----------------------+
     | cluster (var)        |
     +----------------------+
     | resourceLength (4B)  |
     +----------------------+
     | resource (var)       |
     +----------------------+
     | partitionLength (4B) |
     +----------------------+
     | partition (var)      |
     +----------------------+
     | stateLength (4B)     |
     +----------------------+
     | state (var)          |
     +----------------------+
     | instanceLength (4B)  |
     +----------------------+
     | instance (var)       |
     +----------------------+
     | messageLength (4B)   |
     +----------------------+
     |                      |
     | message (var)        |
     |                      |
     +----------------------+

 </pre>
 * </p>
 */
public class NettyHelixActor<T> implements HelixActor<T> {

    private static final Logger LOG = Logger.getLogger(NettyHelixActor.class);
    private static final String ACTOR_PORT = "ACTOR_PORT";
    private static final byte[] EMPTY_BYTES = new byte[0];

    // Parameters for length header field of message (tells decoder to interpret but preserve length field in message)
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final int LENGTH_ADJUSTMENT = -4;
    private static final int INITIAL_BYTES_TO_STRIP = 0;
    private static final int NUM_LENGTH_FIELDS = 7;

    private final AtomicBoolean isShutdown;
    private final ConcurrentMap<InetSocketAddress, Channel> channels;
    private final HelixManager manager;
    private final int port;
    private final HelixActorMessageCodec<T> codec;
    private final HelixActorResolver resolver;
    private final AtomicReference<HelixActorCallback<T>> callback;

    private EventLoopGroup eventLoopGroup;
    private Bootstrap clientBootstrap;
    private NettyHelixActorStats stats;

    /**
     * @param manager
     *  The Helix manager being used by the participant
     * @param port
     *  The port on which to listen for messages
     * @param codec
     *  Codec for decoding / encoding actual message
     */
    public NettyHelixActor(HelixManager manager, int port, HelixActorMessageCodec<T> codec, HelixActorResolver resolver) {
        this.isShutdown = new AtomicBoolean(true);
        this.channels = new ConcurrentHashMap<InetSocketAddress, Channel>();
        this.manager = manager;
        this.port = port;
        this.codec = codec;
        this.resolver = resolver;
        this.callback = new AtomicReference<HelixActorCallback<T>>();
    }

    /**
     * Starts message handling server, creates client bootstrap, and bootstraps partition routing table.
     */
    public void start() throws Exception {
        if (isShutdown.getAndSet(false)) {
            eventLoopGroup = new NioEventLoopGroup();

            stats = new NettyHelixActorStats(eventLoopGroup);
            stats.start();

            ManagementFactory.getPlatformMBeanServer()
                    .registerMBean(stats, new ObjectName(
                            "org.apache.helix:type=NettyHelixActorStats,name=" + manager.getInstanceName()));

            manager.getConfigAccessor().set(
                    new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
                            .forCluster(manager.getClusterName())
                            .forParticipant(manager.getInstanceName())
                            .build(), ACTOR_PORT, String.valueOf(port));

            new ServerBootstrap()
                    .group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new LengthFieldBasedFrameDecoder(
                                    MAX_FRAME_LENGTH,
                                    LENGTH_FIELD_OFFSET,
                                    LENGTH_FIELD_LENGTH,
                                    LENGTH_ADJUSTMENT,
                                    INITIAL_BYTES_TO_STRIP));
                            socketChannel.pipeline().addLast(new HelixActorCallbackHandler());
                        }
                    })
                    .bind(new InetSocketAddress(port));

            clientBootstrap = new Bootstrap()
                    .group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new NopInitializer());
        }
    }

    /**
     * Shuts down event loops for message handling server and message passing client.
     */
    public void shutdown() throws Exception {
        if (!isShutdown.getAndSet(true)) {
            stats.shutdown();
            eventLoopGroup.shutdownGracefully();
        }
    }

    /**
     * Sends a message to all partitions with a given state in the cluster.
     */
    @Override
    public int send(HelixActorScope scope, UUID messageId, T message) {
        // Resolve addresses
        Map<String, InetSocketAddress> addresses = resolver.resolve(scope);

        // Encode message
        ByteBuf messageByteBuf = codec.encode(message);
        byte[] clusterBytes = manager.getClusterName().getBytes();

        // Send message(s)
        for (Map.Entry<String, InetSocketAddress> entry : addresses.entrySet()) {
            try {
                // Get a channel (lazily connect)
                Channel channel = null;
                synchronized (channels) {
                    channel = channels.get(entry.getValue());
                    if (channel == null || !channel.isOpen()) {
                        channel = clientBootstrap.connect(entry.getValue()).sync().channel();
                        channels.put(entry.getValue(), channel);
                        stats.countChannelOpen();
                    }
                }

                // Get metadata bytes
                byte[] resourceBytes = scope.getResource() == null
                        ? EMPTY_BYTES : scope.getResource().getBytes();
                byte[] partitionBytes = scope.getPartition() == null
                        ? EMPTY_BYTES : scope.getPartition().getBytes();
                byte[] stateBytes = scope.getState() == null
                        ? EMPTY_BYTES : scope.getState().getBytes();
                byte[] instanceBytes = entry.getKey().getBytes();

                // Compute total length
                int totalLength = NUM_LENGTH_FIELDS * (Integer.SIZE / 8)
                        + (Long.SIZE / 8) * 2 // 128 bit UUID
                        + clusterBytes.length
                        + resourceBytes.length
                        + partitionBytes.length
                        + stateBytes.length
                        + instanceBytes.length
                        + messageByteBuf.readableBytes();

                // Build message header
                ByteBuf headerBuf = PooledByteBufAllocator.DEFAULT.buffer();
                headerBuf.writeInt(totalLength)
                       .writeLong(messageId.getMostSignificantBits())
                       .writeLong(messageId.getLeastSignificantBits())
                       .writeInt(clusterBytes.length)
                       .writeBytes(clusterBytes)
                       .writeInt(resourceBytes.length)
                       .writeBytes(resourceBytes)
                       .writeInt(partitionBytes.length)
                       .writeBytes(partitionBytes)
                       .writeInt(stateBytes.length)
                       .writeBytes(stateBytes)
                       .writeInt(instanceBytes.length)
                       .writeBytes(instanceBytes)
                       .writeInt(messageByteBuf.readableBytes());

                // Compose message header and payload
                CompositeByteBuf fullByteBuf = new CompositeByteBuf(PooledByteBufAllocator.DEFAULT, false, 2);
                fullByteBuf.addComponent(headerBuf);
                fullByteBuf.addComponent(messageByteBuf);
                fullByteBuf.writerIndex(totalLength);

                // Send
                channel.writeAndFlush(fullByteBuf, channel.voidPromise()); // TODO: No flush to avoid syscall?
                stats.countBytes(totalLength);
                stats.countMessage();
            } catch (Exception e) {
                stats.countError();
                throw new IllegalStateException("Could not send message to " + scope, e);
            }
        }

        return addresses.size();
    }

    /**
     * Register a callback which is called when this node receives a message.
     */
    @Override
    public void register(HelixActorCallback<T> callback) {
        if (!isShutdown.get()) {
            throw new IllegalStateException("Cannot register callback after started");
        }
        this.callback.set(callback);
    }

    // TODO: Avoid creating byte[] and HelixActorScope repeatedly
    // This may be possible using AttributeMap to maintain buffers for the strings,
    // as well as the HelixActorScope object that holds references to them. Should
    // use array-backed ByteBufs. Or it may not be possible. But investigate.
    @ChannelHandler.Sharable
    private class HelixActorCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
            // Message length
            int messageLength = byteBuf.readInt();

            // Message ID
            UUID messageId = new UUID(byteBuf.readLong(), byteBuf.readLong());

            // Cluster
            int clusterSize = byteBuf.readInt();
            if (clusterSize > messageLength) {
                throw new IllegalArgumentException(
                        "nameSize=" + clusterSize + " is greater than messageLength=" + messageLength);
            }
            byte[] clusterBytes = new byte[clusterSize];
            byteBuf.readBytes(clusterBytes);

            // Resource
            int resourceSize = byteBuf.readInt();
            if (resourceSize > messageLength) {
                throw new IllegalArgumentException(
                        "nameSize=" + resourceSize + " is greater than messageLength=" + messageLength);
            }
            byte[] resourceBytes = new byte[resourceSize];
            byteBuf.readBytes(resourceBytes);

            // Partition
            int partitionSize = byteBuf.readInt();
            if (partitionSize > messageLength) {
                throw new IllegalArgumentException(
                        "nameSize=" + partitionSize + " is greater than messageLength=" + messageLength);
            }
            byte[] partitionBytes = new byte[partitionSize];
            byteBuf.readBytes(partitionBytes);

            // State
            int stateSize = byteBuf.readInt();
            if (stateSize > messageLength) {
                throw new IllegalArgumentException(
                        "stateSize=" + stateSize + " is greater than messageLength=" + messageLength);
            }
            byte[] stateBytes = new byte[stateSize];
            byteBuf.readBytes(stateBytes);

            // Instance
            int instanceSize = byteBuf.readInt();
            if (instanceSize > messageLength) {
                throw new IllegalArgumentException(
                        "instanceSize=" + instanceSize + " is greater than messageLength=" + messageLength);
            }
            byte[] instanceBytes = new byte[instanceSize];
            byteBuf.readBytes(instanceBytes);

            // Message
            int messageBytesSize = byteBuf.readInt();
            if (messageBytesSize > messageLength) {
                throw new IllegalArgumentException(
                        "messageBytesSize=" + messageBytesSize + " is greater than messageLength=" + messageLength);
            }
            ByteBuf messageBytes = byteBuf.slice(byteBuf.readerIndex(), messageBytesSize);

            // Parse
            final String clusterName = new String(clusterBytes);
            final String resourceName = new String(resourceBytes);
            final String partitionName = new String(partitionBytes);
            final String state = new String(stateBytes);
            final String instanceName = new String(instanceBytes);
            final T message = codec.decode(messageBytes);

            // Handle callback (must be in this handler to preserve ordering)
            if (instanceName.equals(manager.getInstanceName())) {
                if (callback.get() == null) {
                    throw new IllegalStateException("No callback registered");
                }
                callback.get().onMessage(
                        new HelixActorScope(clusterName, resourceName, partitionName, state), messageId, message);
            } else {
                LOG.warn("Received message addressed to " + instanceName + " which is not this instance");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause) {
            LOG.error(cause);
        }
    }

    private static class NopInitializer extends ChannelInitializer<SocketChannel> {
        private static final ChannelHandler INSTANCE = new NopHandler();
        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            socketChannel.pipeline().addLast(INSTANCE);
        }
    }

    @ChannelHandler.Sharable
    private static class NopHandler extends SimpleChannelInboundHandler<SocketChannel> {
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, SocketChannel socketChannel) throws Exception {
            // NOP
        }
    }
}
