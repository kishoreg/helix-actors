package org.apache.helix.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.helix.*;
import org.apache.helix.ipc.netty.NettyHelixIPCService;
import org.apache.helix.resolver.HelixMessageScope;
import org.apache.helix.resolver.HelixResolver;
import org.apache.helix.resolver.zk.ZKHelixResolver;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestNettyHelixIPCService extends ZkUnitTestBase {

    private static final String CLUSTER_NAME = "TEST_CLUSTER";
    private static final String RESOURCE_NAME = "MyResource";
    private static final HelixIPCMessageCodec<String> CODEC = new HelixIPCMessageCodec<String>() {
        @Override
        public ByteBuf encode(String message) {
            return Unpooled.wrappedBuffer(message.getBytes());
        }

        @Override
        public String decode(ByteBuf message) {
            byte[] bytes = new byte[message.readableBytes()]; // n.b. this is bad
            message.readBytes(bytes);
            return new String(bytes);
        }
    };

    private int firstPort;
    private int secondPort;
    private HelixManager controller;
    private HelixManager firstNode;
    private HelixManager secondNode;
    private HelixResolver firstResolver;
    private HelixResolver secondResolver;

    @BeforeClass
    public void beforeClass() throws Exception {
        // Allocate test resources
        firstPort = TestHelper.getRandomPort();
        secondPort = TestHelper.getRandomPort();

        // Setup cluster
        ClusterSetup clusterSetup = new ClusterSetup(ZK_ADDR);
        clusterSetup.addCluster(CLUSTER_NAME, true);
        clusterSetup.addInstanceToCluster(CLUSTER_NAME, "localhost_" + firstPort);
        clusterSetup.addInstanceToCluster(CLUSTER_NAME, "localhost_" + secondPort);

        // Start Helix agents
        controller = HelixControllerMain.startHelixController(ZK_ADDR, CLUSTER_NAME, "CONTROLLER", "STANDALONE");
        firstNode = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + firstPort, InstanceType.PARTICIPANT, ZK_ADDR);
        secondNode = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + secondPort, InstanceType.PARTICIPANT, ZK_ADDR);

        // Connect participants
        firstNode.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new DummyStateModelFactory());
        firstNode.connect();
        secondNode.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new DummyStateModelFactory());
        secondNode.connect();

        // Add a resource
        clusterSetup.addResourceToCluster(CLUSTER_NAME, RESOURCE_NAME, 4, "OnlineOffline");
        clusterSetup.rebalanceResource(CLUSTER_NAME, RESOURCE_NAME, 1);

        // Wait for External view convergence
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME), 10000);

        // Connect resolvers
        firstResolver = new ZKHelixResolver(ZK_ADDR);
        firstResolver.connect();
        secondResolver = new ZKHelixResolver(ZK_ADDR);
        secondResolver.connect();
    }

    @AfterClass
    public void afterClass() throws Exception {
        firstNode.disconnect();
        secondNode.disconnect();
        controller.disconnect();
    }

    @Test
    public void testMessagePassing() throws Exception {
        int numMessages = 1000;

        // Start first IPC service w/ counter
        final ConcurrentMap<String, AtomicInteger> firstCounts = new ConcurrentHashMap<String, AtomicInteger>();
        NettyHelixIPCService<String> firstIPC = new NettyHelixIPCService<String>(firstNode, firstPort, CODEC, firstResolver);
        firstIPC.register(new HelixIPCCallback<String>() {
            @Override
            public void onMessage(HelixMessageScope scope, UUID messageId, String message) {
                String key = scope.getPartition() + ":" + scope.getState();
                firstCounts.putIfAbsent(key, new AtomicInteger());
                firstCounts.get(key).incrementAndGet();
            }
        });
        firstIPC.start();

        // Start second IPC Service w/ counter
        final ConcurrentMap<String, AtomicInteger> secondCounts = new ConcurrentHashMap<String, AtomicInteger>();
        NettyHelixIPCService<String> secondIPC = new NettyHelixIPCService<String>(secondNode, secondPort, CODEC, secondResolver);
        secondIPC.register(new HelixIPCCallback<String>() {
            @Override
            public void onMessage(HelixMessageScope scope, UUID messageId, String message) {
                String key = scope.getPartition() + ":" + scope.getState();
                secondCounts.putIfAbsent(key, new AtomicInteger());
                secondCounts.get(key).incrementAndGet();
            }
        });
        secondIPC.start();

        // Allow resolver callbacks to fire
        Thread.sleep(500);

        // Find all partitions on second node...
        String secondName = "localhost_" + secondPort;
        Set<String> secondPartitions = new HashSet<String>();
        IdealState idealState = controller.getClusterManagmentTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
        for (String partitionName : idealState.getPartitionSet()) {
            for (Map.Entry<String, String> stateEntry : idealState.getInstanceStateMap(partitionName).entrySet()) {
                if (stateEntry.getKey().equals(secondName)) {
                    secondPartitions.add(partitionName);
                }
            }
        }

        // And use first node to send messages to them
        for (String partitionName : secondPartitions) {
            for (int i = 0; i < numMessages; i++) {
                firstIPC.send(new HelixMessageScope.Builder()
                        .cluster(firstNode.getClusterName())
                        .resource(RESOURCE_NAME)
                        .partition(partitionName)
                        .state("ONLINE").build(), UUID.randomUUID(), "Hello world " + i);
            }
        }

        // Loopback
        for (String partitionName : secondPartitions) {
            for (int i = 0; i < numMessages; i++) {
                secondIPC.send(new HelixMessageScope.Builder()
                        .cluster(secondNode.getClusterName())
                        .resource(RESOURCE_NAME)
                        .partition(partitionName)
                        .state("ONLINE").build(), UUID.randomUUID(), "Hello world " + i);
            }
        }

        // Check
        Thread.sleep(100); // just in case
        for (String partitionName : secondPartitions) {
            AtomicInteger count = secondCounts.get(partitionName + ":ONLINE");
            Assert.assertNotNull(count);
            Assert.assertEquals(count.get(), 2 * numMessages);
        }
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
