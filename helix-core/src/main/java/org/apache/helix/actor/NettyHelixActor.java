package org.apache.helix.actor;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides partition/state-level messaging among nodes in a Helix cluster.
 *
 * <p>
 *     The message format is
 <pre>
     +----------------------+
     | totalLength (4B)     |
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

 TODO: Move implementation into a different module, but leave interface in core

 </pre>
 * </p>
 */
// TODO: Hierarchy of callbacks for different scopes
public class NettyHelixActor<T> implements HelixActor<T> {

    private static final Logger LOG = Logger.getLogger(NettyHelixActor.class);
    private static final String ACTOR_PORT = "ACTOR_PORT";

    // Parameters for length header field of message (tells decoder to interpret but preserve length field in message)
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final int LENGTH_ADJUSTMENT = -4;
    private static final int INITIAL_BYTES_TO_STRIP = 0;
    private static final int NUM_LENGTH_FIELDS = 7;

    private final AtomicBoolean isShutdown;
    private final ConcurrentMap<String, HelixActorCallback<T>> callbacks;
    private final ConcurrentMap<InetSocketAddress, Channel> channels;
    private final HelixManager manager;
    private final int port;
    private final HelixActorMessageCodec<T> codec;
    private final RoutingTableProvider routingTableProvider;

    private EventLoopGroup eventLoopGroup;
    private Bootstrap clientBootstrap;

    /**
     * @param manager
     *  The Helix manager being used by the participant
     * @param port
     *  The port on which to listen for messages
     * @param codec
     *  Codec for decoding / encoding actual message
     */
    public NettyHelixActor(HelixManager manager, int port, HelixActorMessageCodec<T> codec) {
        this.isShutdown = new AtomicBoolean(true);
        this.callbacks = new ConcurrentHashMap<String, HelixActorCallback<T>>();
        this.channels = new ConcurrentHashMap<InetSocketAddress, Channel>();
        this.manager = manager;
        this.port = port;
        this.codec = codec;
        this.routingTableProvider = new RoutingTableProvider();
    }

    /**
     * Starts message handling server, creates client bootstrap, and bootstraps partition routing table.
     */
    public void start() throws Exception {
        if (isShutdown.getAndSet(false)) {
            eventLoopGroup = new NioEventLoopGroup();

            manager.getConfigAccessor().set(
                    new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
                            .forCluster(manager.getClusterName())
                            .forParticipant(manager.getInstanceName())
                            .build(), ACTOR_PORT, String.valueOf(port));

            manager.addExternalViewChangeListener(routingTableProvider);
            manager.addInstanceConfigChangeListener(routingTableProvider);

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

            // Bootstrap routing table
            routingTableProvider.onExternalViewChange(getExternalViews(), new NotificationContext(manager));
        }
    }

    /**
     * Shuts down event loops for message handling server and message passing client.
     */
    public void shutdown() throws Exception {
        if (!isShutdown.getAndSet(true)) {
            eventLoopGroup.shutdownGracefully();
        }
    }

    /**
     * Sends a message to all partitions with a given state in the cluster.
     */
    // TODO: Make address builder thing to make this easy
    @Override
    public void send(String resource, String partition, String state, T message) {
        // Get addresses
        Map<String, InetSocketAddress> addresses = new HashMap<String, InetSocketAddress>();
        for (InstanceConfig instanceConfig : routingTableProvider.getInstances(resource, partition, state)) {
            String actorPort = instanceConfig.getRecord().getSimpleField(ACTOR_PORT);
            if (actorPort == null) {
                throw new IllegalStateException("No actor address registered for target instance " + instanceConfig.getInstanceName());
            }
            addresses.put(instanceConfig.getInstanceName(), new InetSocketAddress(instanceConfig.getHostName(), Integer.valueOf(actorPort)));
        }

        // Encode message
        byte[] messageBytes = codec.encode(message);
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
                    }
                }

                byte[] resourceBytes = resource.getBytes();
                byte[] partitionBytes = partition.getBytes();
                byte[] stateBytes = state.getBytes();
                byte[] instanceBytes = entry.getKey().getBytes();
                int totalLength = NUM_LENGTH_FIELDS * (Integer.SIZE / 8)
                        + clusterBytes.length
                        + resourceBytes.length
                        + partitionBytes.length
                        + stateBytes.length
                        + instanceBytes.length
                        + messageBytes.length;

                // Build and send message
                ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
                byteBuf.writeInt(totalLength)
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
                       .writeInt(messageBytes.length)
                       .writeBytes(messageBytes);
                channel.writeAndFlush(byteBuf, channel.voidPromise()); // TODO: No flush to avoid syscall?
            } catch (Exception e) {
                throw new IllegalStateException("Could not send message to " + partition + ":" + state, e);
            }
        }
    }

    /**
     * Register a callback which is called when this node receives a message for any partition/state of a resource.
     *
     * <p>
     *     Only one callback may be registerd per resource. A subsequent call to this method results in the most
     *     recently supplied callback being the one that's registered.
     * </p>
     */
    public void register(String resource, HelixActorCallback<T> callback) {
        callbacks.put(resource, callback);
    }

    // Returns external views for all resources currently in cluster
    private List<ExternalView> getExternalViews() {
        List<String> resources = manager.getClusterManagmentTool().getResourcesInCluster(manager.getClusterName());
        List<ExternalView> externalViews = new ArrayList<ExternalView>(resources.size());
        for (String resource : resources) {
            ExternalView externalView = manager.getClusterManagmentTool().getResourceExternalView(manager.getClusterName(), resource);
            if (externalView != null) {
                externalViews.add(externalView);
            }
        }
        return externalViews;
    }

    // Routes received messages to their respective callbacks
    @ChannelHandler.Sharable
    private class HelixActorCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
            // Message length
            int messageLength = byteBuf.readInt();

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
            byte[] messageBytes = new byte[messageBytesSize];
            byteBuf.readBytes(messageBytes);

            // Parse
            final String partitionName = new String(partitionBytes);
            final String state = new String(stateBytes);
            final String instanceName = new String(instanceBytes);
            final T message = codec.decode(messageBytes);

            // Handle callback (don't block this handler b/c callback may be expensive)
            if (instanceName.equals(manager.getInstanceName())) {
                String resourceName = partitionName.substring(0, partitionName.lastIndexOf("_"));
                final HelixActorCallback<T> callback = callbacks.get(resourceName);
                if (callback == null) {
                    throw new IllegalStateException("No callback registered for resource " + resourceName);
                }
                ctx.channel().eventLoop().submit(new Runnable() {
                    @Override
                    public void run() {
                        callback.onMessage(new Partition(partitionName), state, message);
                    }
                });
            } else {
                LOG.warn("Received message addressed to " + instanceName + " which is not this instance");
            }

            // Done with those
            byteBuf.discardReadBytes();
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
