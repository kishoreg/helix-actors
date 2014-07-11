package org.apache.helix.actor;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.helix.*;
import org.apache.helix.model.*;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides partition/state-level messaging among nodes in a Helix cluster.
 *
 * <p>
 *     Message format is: [ nameLength (4B) | name (var) | stateLength (4B) | state (var) | message (var) ]
 * </p>
 *
 * <p>
 *     This module accepts a typed {@link HelixActorMessageCodec}, which is used to serialize / deserialize
 *     message body.
 * </p>
 */
public class NettyHelixActor<T> implements HelixActor<T> {

    private static final Logger LOG = Logger.getLogger(NettyHelixActor.class);
    private static final String ACTOR_PORT = "ACTOR_PORT";

    private final AtomicBoolean isShutdown;
    private final ConcurrentMap<String, HelixActorCallback<T>> callbacks;
    private final ConcurrentMap<String, InetSocketAddress> ipcAddresses;
    private final HelixManager manager;
    private final int port;
    private final HelixActorMessageCodec<T> codec;

    private EventLoopGroup serverEventLoopGroup;
    private EventLoopGroup clientEventLoopGroup;
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
        this.ipcAddresses = new ConcurrentHashMap<String, InetSocketAddress>();
        this.manager = manager;
        this.port = port;
        this.codec = codec;
    }

    /**
     * Starts message handling server, creates client bootstrap, and bootstraps partition routing table.
     */
    public void start() {
        if (isShutdown.getAndSet(false)) {
            serverEventLoopGroup = new NioEventLoopGroup();
            clientEventLoopGroup = new NioEventLoopGroup();

            new ServerBootstrap()
                    .group(serverEventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new HelixActorCallbackHandler());
                        }
                    })
                    .bind(new InetSocketAddress(port));

            clientBootstrap = new Bootstrap()
                    .group(clientEventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new SimpleChannelInboundHandler<SocketChannel>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext channelHandlerContext, SocketChannel socketChannel) throws Exception {
                                    // Do nothing
                                }
                            });
                        }
                    });

            // Bootstrap routing table
            List<String> resources = manager.getClusterManagmentTool().getResourcesInCluster(manager.getClusterName());
            List<ExternalView> externalViews = new ArrayList<ExternalView>(resources.size());
            for (String resource : resources) {
                ExternalView externalView = manager.getClusterManagmentTool().getResourceExternalView(manager.getClusterName(), resource);
                if (externalView != null) {
                    externalViews.add(externalView);
                }
            }
            onExternalViewChange(externalViews, null);
        }
    }

    /**
     * Shuts down event loops for message handling server and message passing client.
     */
    public void shutdown() {
        if (isShutdown.getAndSet(true)) {
            clientEventLoopGroup.shutdownGracefully();
            serverEventLoopGroup.shutdownGracefully();
        }
    }

    /**
     * Sends a message to all partitions with a given state in the cluster.
     */
    public void send(Partition partition, String state, T message) {
        // Get addresses
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        synchronized (ipcAddresses) {
            for (String instance : getInstances(partition, state)) {
                InetSocketAddress address = ipcAddresses.get(instance);
                if (address == null) {
                    throw new IllegalStateException("No actor address for target instance " + instance);
                }
                addresses.add(address);
            }
        }

        // Encode and wrap message
        String partitionName = partition.getPartitionName();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(
                ByteBuffer.allocate(4).putInt(partitionName.length()).array(),
                partitionName.getBytes(),
                ByteBuffer.allocate(4).putInt(state.length()).array(),
                state.getBytes(),
                this.codec.encode(message));

        // Send message(s)
        for (final InetSocketAddress address : addresses) {
            try {
                // TODO: Reuse channels - just for simplicity now
                Channel channel = clientBootstrap.connect(address).sync().channel();
                channel.writeAndFlush(byteBuf).addListener(ChannelFutureListener.CLOSE);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Could not send message to " + partition + ":" + state, e);
            }
        }
    }

    /**
     * Register a callback which is called when this node receives a message for any partition/state of a resource.
     */
    public void register(String resource, HelixActorCallback<T> callback) {
        callbacks.put(resource, callback);
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        synchronized (ipcAddresses) {
            ipcAddresses.clear();
            for (ExternalView externalView : externalViewList) {
                Set<String> partitions = externalView.getPartitionSet();
                if (partitions != null) {
                    for (String partitionName : partitions) {
                        for (Map.Entry<String, String> stateEntry : externalView.getStateMap(partitionName).entrySet()) {
                            String instance = stateEntry.getKey();
                            InstanceConfig config = manager.getClusterManagmentTool().getInstanceConfig(manager.getClusterName(), instance);
                            String actorPort = config.getRecord().getSimpleField(ACTOR_PORT);
                            if (actorPort != null) {
                                String host = instance.substring(0, instance.lastIndexOf("_"));
                                ipcAddresses.put(instance, new InetSocketAddress(host, Integer.parseInt(actorPort)));
                            }
                        }
                    }
                }
            }
        }
    }

    // Given a partition and its state, find all instances that are in that state currently
    private List<String> getInstances(Partition partition, String state) {
        List<String> instances = new ArrayList<String>();
        String partitionName = partition.getPartitionName();
        String resourceName = partitionName.substring(0, partitionName.lastIndexOf("_"));
        ExternalView externalView = manager.getClusterManagmentTool().getResourceExternalView(manager.getClusterName(), resourceName);
        if (externalView != null) {
            Map<String, String> stateMap = externalView.getStateMap(partitionName);
            if (stateMap != null) {
                for (Map.Entry<String, String> stateEntry : stateMap.entrySet()) {
                    if (stateEntry.getValue().equals(state)) {
                        instances.add(stateEntry.getKey());
                    }
                }
            }
        }
        return instances;
    }

    // Routes received messages to their respective callbacks
    private class HelixActorCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
            // Partition name
            int nameSize = byteBuf.readInt();
            byte[] nameBytes = new byte[nameSize];
            byteBuf.readBytes(nameBytes);

            // Partition state
            int stateSize = byteBuf.readInt();
            byte[] stateBytes = new byte[stateSize];
            byteBuf.readBytes(stateBytes);

            // Message
            byte[] messageBytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(messageBytes);

            // Parse
            String partitionName = new String(nameBytes);
            String resourceName = partitionName.substring(0, partitionName.lastIndexOf("_"));
            String state = new String(stateBytes);
            T message = codec.decode(messageBytes);

            // Handle callback
            HelixActorCallback<T> callback = callbacks.get(resourceName);
            if (callback == null) {
                throw new IllegalStateException("No callback registered for resource " + resourceName);
            }
            callback.onMessage(new Partition(partitionName), state, message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause) {
            LOG.error(cause);
        }
    }
}
