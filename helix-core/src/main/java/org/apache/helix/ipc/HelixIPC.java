package org.apache.helix.ipc;

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
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.*;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides partition-level messaging among nodes in a Helix cluster.
 *
 * <p>
 *     Message format is: [ nameLength (4B) | name (var) | stateLength (4B) | state (var) | message (var) ]
 * </p>
 */
public class HelixIPC<T> implements ExternalViewChangeListener {

    private static final Logger LOG = Logger.getLogger(HelixIPC.class);

    private final AtomicBoolean isShutdown;
    private final ConcurrentMap<Partition, Map<String, HelixIPCCallback<T>>> callbacks;
    private final ConcurrentMap<String, InetSocketAddress> ipcAddresses;
    private final HelixManager manager;
    private final int port;
    private final HelixIPCMessageCodec<T> codec;

    private EventLoopGroup serverEventLoopGroup;
    private EventLoopGroup clientEventLoopGroup;
    private Bootstrap clientBootstrap;

    public HelixIPC(HelixManager manager, int port, HelixIPCMessageCodec<T> codec) {
        this.isShutdown = new AtomicBoolean(true);
        this.callbacks = new ConcurrentHashMap<Partition, Map<String, HelixIPCCallback<T>>>();
        this.ipcAddresses = new ConcurrentHashMap<String, InetSocketAddress>();
        this.manager = manager;
        this.port = port;
        this.codec = codec;
    }

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
                            socketChannel.pipeline().addLast(new HelixIPCCallbackHandler());
                        }
                    })
                    .bind(new InetSocketAddress(port));

            clientBootstrap = new Bootstrap()
                    .group(clientEventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new SimpleChannelInboundHandler<SocketChannel>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext channelHandlerContext, SocketChannel socketChannel) throws Exception {
                            // Do nothing
                        }
                    });

            bootstrapRoutingTable();
        }
    }

    public void shutdown() {
        if (isShutdown.getAndSet(true)) {
            serverEventLoopGroup.shutdownGracefully();
        }
    }

    public void send(Partition partition, String state, T message) throws IOException, InterruptedException {
        // Get addresses
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String instance : getInstances(partition, state)) {
            InetSocketAddress address = ipcAddresses.get(instance);
            if (address == null) {
                throw new IllegalStateException("No IPC address for target instance " + instance);
            }
            addresses.add(address);
        }

        // Encode and wrap message
        String partitionName = partition.getPartitionName();
        byte[] messageBytes = codec.encode(message);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(
                ByteBuffer.allocate(4).putInt(partitionName.length()).array(),
                partitionName.getBytes(),
                ByteBuffer.allocate(4).putInt(state.length()).array(),
                state.getBytes(),
                messageBytes);

        // Send message(s)
        for (InetSocketAddress address : addresses) {
            Channel channel = clientBootstrap.connect(address).sync().channel();
            channel.writeAndFlush(byteBuf).addListener(ChannelFutureListener.CLOSE);
        }
    }

    public void register(Partition partition, String state, HelixIPCCallback<T> callback) {
        Map<String, HelixIPCCallback<T>> stateMap = callbacks.get(partition);
        if (stateMap == null) {
            stateMap = new HashMap<String, HelixIPCCallback<T>>();
            callbacks.put(partition, stateMap);
        }
        stateMap.put(state, callback);
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        bootstrapRoutingTable();
    }

    // Builds mapping of instance name to IPC socket address
    private void bootstrapRoutingTable() {
        List<String> resources = manager.getClusterManagmentTool().getResourcesInCluster(manager.getClusterName());
        for (String resource : resources) {
            ExternalView externalView
                    = manager.getClusterManagmentTool().getResourceExternalView(manager.getClusterName(), resource);
            if (externalView != null) {
                Set<String> partitions = externalView.getPartitionSet();
                if (partitions != null) {
                    for (String partitionName : partitions) {
                        for (Map.Entry<String, String> stateEntry : externalView.getStateMap(partitionName).entrySet()) {
                            String instance = stateEntry.getKey();
                            InstanceConfig instanceConfig = manager.getClusterManagmentTool().getInstanceConfig(manager.getClusterName(), instance);
                            String ipcPort = instanceConfig.getRecord().getSimpleField("IPC_PORT");
                            if (ipcPort != null) {
                                String host = instance.substring(0, instance.lastIndexOf("_"));
                                ipcAddresses.put(instance, new InetSocketAddress(host, Integer.parseInt(ipcPort)));
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
        for (Map.Entry<String, String> stateEntry : externalView.getStateMap(partitionName).entrySet()) {
            if (stateEntry.getValue().equals(state)) {
                instances.add(stateEntry.getKey());
            }
        }
        return instances;
    }

    private class HelixIPCCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {
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
            Partition partition = new Partition(new String(nameBytes));
            String state = new String(stateBytes);
            T message = codec.decode(messageBytes);

            // Handle callback
            Map<String, HelixIPCCallback<T>> stateMap = callbacks.get(partition);
            if (stateMap == null) {
                throw new IllegalStateException("No callback registered for partition " + partition);
            }
            HelixIPCCallback<T> callback = stateMap.get(state);
            if (callback == null) {
                throw new IllegalStateException("No callback registered for partition " + partition + " and state " + state);
            }
            callback.onMessage(partition, message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause) {
            LOG.error(cause);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        HelixIPCMessageCodec<String> codec = new HelixIPCMessageCodec<String>() {
            @Override
            public byte[] encode(String message) {
                return message.getBytes();
            }

            @Override
            public String decode(byte[] message) {
                return new String(message);
            }
        };

        ClusterSetup clusterSetup = new ClusterSetup("localhost:2181");
        clusterSetup.addCluster("TEST_CLUSTER", true);
        clusterSetup.addInstanceToCluster("TEST_CLUSTER", "localhost_8000");
        clusterSetup.addInstanceToCluster("TEST_CLUSTER", "localhost_9000");
        clusterSetup.addResourceToCluster("TEST_CLUSTER", "MyDB", 1, "OnlineOffline");
        clusterSetup.rebalanceResource("TEST_CLUSTER", "MyDB", 1);
        clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, "TEST_CLUSTER,localhost_8000", "IPC_PORT=8000");
        clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, "TEST_CLUSTER,localhost_9000", "IPC_PORT=9000");

        HelixControllerMain.startHelixController("localhost:2181", "TEST_CLUSTER", "CONTROLLER", "STANDALONE");

        HelixManager manager1 = HelixManagerFactory.getZKHelixManager("TEST_CLUSTER", "localhost_8000", InstanceType.PARTICIPANT, "localhost:2181");
        manager1.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new DummyStateModelFactory());
        manager1.connect();

        HelixManager manager2 = HelixManagerFactory.getZKHelixManager("TEST_CLUSTER", "localhost_9000", InstanceType.PARTICIPANT, "localhost:2181");
        manager2.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new DummyStateModelFactory());
        manager2.connect();

        HelixIPC<String> ipc1 = new HelixIPC<String>(manager1, 8000, codec);
        HelixIPC<String> ipc2 = new HelixIPC<String>(manager2, 8000, codec);

        HelixIPCCallback<String> callback = new HelixIPCCallback<String>() {
            @Override
            public void onMessage(Partition partition, String message) {
                System.out.println(partition + ": " + message);
            }
        };

        Partition partition = new Partition("MyDB_0");
        ipc1.register(partition, "ONLINE", callback);
        ipc2.register(partition, "ONLINE", callback);

        ipc1.start();
        ipc2.start();

        ipc2.send(partition, "ONLINE", "Hello MyDB_0! Coming from ipc2");
    }

    public static class DummyStateModelFactory extends StateModelFactory<StateModel> {
        @Override
        public StateModel createNewStateModel(String partitionName) {
            return new DummyStateModel();
        }

        @StateModelInfo(states = "{'OFFLINE', 'ONLINE'}", initialState = "OFFLINE")
        public static class DummyStateModel extends StateModel {
            @Transition(from = "OFFLINE", to = "ONLINE")
            public void fromOfflineToOnline(Message message, NotificationContext context) {
                System.out.println(message);
            }

            @Transition(from = "ONLINE", to = "OFFLINE")
            public void fromOnlineToOffline(Message message, NotificationContext context) {
                System.out.println(message);
            }
        }
    }
}
