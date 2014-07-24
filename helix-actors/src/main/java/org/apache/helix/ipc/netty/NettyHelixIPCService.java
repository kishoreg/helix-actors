package org.apache.helix.ipc.netty;

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
import org.apache.helix.ipc.AbstractHelixIPCService;
import org.apache.helix.ipc.HelixIPCConstants;
import org.apache.helix.ipc.HelixIPCMessageCodec;
import org.apache.helix.resolver.HelixMessageScope;
import org.apache.log4j.Logger;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides partition/state-level messaging among nodes in a Helix cluster.
 *
 * <p>
 *     The message format is (where len == 4B, and contains the length of the next field)
 <pre>
     +----------------------+
     | totalLength (4B)     |
     +----------------------+
     | version (4B)         |
     +----------------------+
     | messageType (4B)     |
     +----------------------+
     | messageId (16B)      |
     +----------------------+
     | len | cluster        |
     +----------------------+
     | len | resource       |
     +----------------------+
     | len | partition      |
     +----------------------+
     | len | state          |
     +----------------------+
     | len | srcInstance    |
     +----------------------+
     | len | dstInstance    |
     +----------------------+
     | len | message        |
     +----------------------+
 </pre>
 * </p>
 */
public class NettyHelixIPCService extends AbstractHelixIPCService {

    private static final Logger LOG = Logger.getLogger(NettyHelixIPCService.class);
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int MESSAGE_VERSION = 1;

    // Parameters for length header field of message (tells decoder to interpret but preserve length field in message)
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final int LENGTH_ADJUSTMENT = -4;
    private static final int INITIAL_BYTES_TO_STRIP = 0;
    private static final int NUM_LENGTH_FIELDS = 8;

    private final AtomicBoolean isShutdown;
    private final ConcurrentMap<InetSocketAddress, Channel> channels;
    private final boolean shouldFlush;

    private EventLoopGroup eventLoopGroup;
    private Bootstrap clientBootstrap;
    private NettyHelixIPCStats stats;

    public NettyHelixIPCService(String instanceName, int port) {
        this(instanceName, port, true);
    }

    /**
     * @param instanceName
     *  The Helix instance name (used as source instance name)
     * @param port
     *  The port on which to listen for other IPC messages
     * @param shouldFlush
     *  If true, {@link Channel#writeAndFlush(Object)} is called when IPC messages are sent,
     *  (that is, the message makes it out on the wire after {@link #send} returns). If false,
     *  messages are buffered and flushed asynchronously. In high-throughput applications, one
     *  should set shouldFlush to false to avoid the expensive system call involved.
     */
    public NettyHelixIPCService(String instanceName, int port, boolean shouldFlush) {
        super(instanceName, port);
        this.isShutdown = new AtomicBoolean(true);
        this.channels = new ConcurrentHashMap<InetSocketAddress, Channel>();
        this.shouldFlush = shouldFlush;
    }

    /**
     * Starts message handling server, creates client bootstrap, and bootstraps partition routing table.
     */
    public void start() throws Exception {
        if (isShutdown.getAndSet(false)) {
            eventLoopGroup = new NioEventLoopGroup();

            stats = new NettyHelixIPCStats(eventLoopGroup);
            stats.start();

            ManagementFactory.getPlatformMBeanServer()
                    .registerMBean(stats, new ObjectName(
                            "org.apache.helix:type=NettyHelixIPCStats,name=" + instanceName));

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
                            socketChannel.pipeline().addLast(new HelixActorIPCCallbackHandler());
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
    public void send(HelixMessageScope scope,
                     int messageType,
                     UUID messageId,
                     Object message) {
        // Get codec
        HelixIPCMessageCodec codec = messageCodecs.get(messageType);
        if (codec == null) {
            throw new IllegalArgumentException("No codec for message type " + messageType);
        }

        // Encode message
        ByteBuf messageByteBuf = codec.encode(message);
        byte[] clusterBytes = scope.getCluster() == null ?
                EMPTY_BYTES : scope.getCluster().getBytes();

        // Send message(s)
        if (scope.getDestinationAddresses() == null) {
            throw new IllegalArgumentException("Cannot send message to unresolved scope");
        }
        for (Map.Entry<String, InetSocketAddress> entry : scope.getDestinationAddresses().entrySet()) {
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
                byte[] srcInstanceBytes = instanceName.getBytes();
                byte[] dstInstanceBytes = entry.getKey().getBytes();

                // Compute total length
                int totalLength = NUM_LENGTH_FIELDS * (Integer.SIZE / 8)
                        + (Integer.SIZE / 8) * 2 // version, type
                        + (Long.SIZE / 8) * 2 // 128 bit UUID
                        + clusterBytes.length
                        + resourceBytes.length
                        + partitionBytes.length
                        + stateBytes.length
                        + srcInstanceBytes.length
                        + dstInstanceBytes.length
                        + messageByteBuf.readableBytes();

                // Build message header
                ByteBuf headerBuf = PooledByteBufAllocator.DEFAULT.buffer();
                headerBuf.writeInt(totalLength)
                         .writeInt(MESSAGE_VERSION)
                         .writeInt(messageType)
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
                         .writeInt(srcInstanceBytes.length)
                         .writeBytes(srcInstanceBytes)
                         .writeInt(dstInstanceBytes.length)
                         .writeBytes(dstInstanceBytes)
                         .writeInt(messageByteBuf.readableBytes());

                // Compose message header and payload
                CompositeByteBuf fullByteBuf = new CompositeByteBuf(PooledByteBufAllocator.DEFAULT, false, 2);
                fullByteBuf.addComponent(headerBuf);
                fullByteBuf.addComponent(messageByteBuf);
                fullByteBuf.writerIndex(totalLength);

                // Send
                if (shouldFlush) {
                    channel.writeAndFlush(fullByteBuf, channel.voidPromise());
                } else {
                    channel.write(fullByteBuf, channel.voidPromise());
                }
                stats.countBytes(totalLength);
                stats.countMessage();
            } catch (Exception e) {
                stats.countError();
                throw new IllegalStateException("Could not send message to " + scope, e);
            }
        }
    }

    @Override
    public void ack(HelixMessageScope scope, UUID messageId) {
        if (scope.getSourceAddress() == null) {
            throw new IllegalArgumentException("Cannot ack message to unresolved scope");
        }

        // Compute message length
        int totalLength = NUM_LENGTH_FIELDS * (Integer.SIZE / 8)
                + (Integer.SIZE / 8) * 2 // version, type
                + (Long.SIZE / 8) * 2; // 128 bit UUID

        // Build message header
        ByteBuf headerBuf = PooledByteBufAllocator.DEFAULT.buffer();
        headerBuf.writeInt(totalLength)
                .writeInt(MESSAGE_VERSION)
                .writeInt(HelixIPCConstants.MESSAGE_TYPE_ACK)
                .writeLong(messageId.getMostSignificantBits())
                .writeLong(messageId.getLeastSignificantBits())
                .writeInt(0)
                .writeInt(0)
                .writeInt(0)
                .writeInt(0)
                .writeInt(0)
                .writeInt(0);

        // Get a channel (lazily connect)
        Channel channel = null;
        try {
            synchronized (channels) {
                channel = channels.get(scope.getSourceAddress());
                if (channel == null || !channel.isOpen()) {
                    channel = clientBootstrap.connect(scope.getSourceAddress()).sync().channel();
                    channels.put(scope.getSourceAddress(), channel);
                    stats.countChannelOpen();
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Could not connect to " + scope.getSourceAddress());
        }

        // Send
        if (shouldFlush) {
            channel.writeAndFlush(headerBuf, channel.voidPromise());
        } else {
            channel.write(headerBuf, channel.voidPromise());
        }
        stats.countBytes(totalLength);
        stats.countMessage();
    }

    // TODO: Avoid creating byte[] and HelixActorScope repeatedly
    // This may be possible using AttributeMap to maintain buffers for the strings,
    // as well as the HelixActorScope object that holds references to them. Should
    // use array-backed ByteBufs. Or it may not be possible. But investigate.
    @ChannelHandler.Sharable
    private class HelixActorIPCCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
            // Message length
            int messageLength = byteBuf.readInt();

            // Message version
            int messageVersion = byteBuf.readInt();

            // Message type
            int messageType = byteBuf.readInt();
            HelixIPCMessageCodec codec = messageCodecs.get(messageType);
            if (codec == null) {
                throw new IllegalStateException("Received message for which there is no codec: type=" + messageType);
            }

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

            // Source instance
            int srcInstanceSize = byteBuf.readInt();
            if (srcInstanceSize > messageLength) {
                throw new IllegalArgumentException(
                        "instanceSize=" + srcInstanceSize + " is greater than messageLength=" + messageLength);
            }
            byte[] srcInstanceBytes = new byte[srcInstanceSize];
            byteBuf.readBytes(srcInstanceBytes);

            // Destination instance
            int dstInstanceSize = byteBuf.readInt();
            if (dstInstanceSize > messageLength) {
                throw new IllegalArgumentException(
                        "instanceSize=" + dstInstanceSize + " is greater than messageLength=" + messageLength);
            }
            byte[] dstInstanceBytes = new byte[dstInstanceSize];
            byteBuf.readBytes(dstInstanceBytes);

            // Message
            int messageBytesSize = byteBuf.readInt();
            if (messageBytesSize > messageLength) {
                throw new IllegalArgumentException(
                        "messageBytesSize=" + messageBytesSize + " is greater than messageLength=" + messageLength);
            }
            ByteBuf messageBytes = byteBuf.slice(byteBuf.readerIndex(), messageBytesSize);

            // Parse
            String clusterName = toNonEmptyString(clusterBytes);
            String resourceName = toNonEmptyString(resourceBytes);
            String partitionName = toNonEmptyString(partitionBytes);
            String state = toNonEmptyString(stateBytes);
            String srcInstance = toNonEmptyString(srcInstanceBytes);
            String dstInstance = toNonEmptyString(dstInstanceBytes);
            Object message = codec.decode(messageBytes);

            // Error check
            if (dstInstance == null) {
                throw new IllegalStateException(
                    "Received message addressed to null destination from " + srcInstance);
            } else if (!dstInstance.equals(instanceName)) {
                throw new IllegalStateException(
                    "Received message addressed to " + dstInstance + " from " + srcInstance);
            } else if (callbacks.get(messageType) == null) {
                throw new IllegalStateException(
                    "No callback registered for message type " + messageType);
            }

            // Handle callback (must be done in this handler to preserve message ordering)
            callbacks.get(messageType).onMessage(
                    new HelixMessageScope.Builder()
                            .cluster(clusterName)
                            .resource(resourceName)
                            .partition(partitionName)
                            .state(state)
                            .sourceInstance(srcInstance)
                            .build(),
                    messageId,
                    message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause) {
            LOG.error(cause);
        }
    }

    // Returns null if bytes.length == 0, or a String from those bytes
    private static String toNonEmptyString(byte[] bytes) {
        return bytes.length > 0 ? new String(bytes) : null;
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
