package org.apache.helix.actor.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.actor.api.HelixActorCallback;
import org.apache.helix.actor.api.HelixActorMessageCodec;
import org.apache.helix.actor.api.HelixActorScope;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
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

public class TestNettyHelixActor extends ZkUnitTestBase {

    private static final String CLUSTER_NAME = "TEST_CLUSTER";
    private static final String RESOURCE_NAME = "MyResource";
    private static final HelixActorMessageCodec<String> CODEC = new HelixActorMessageCodec<String>() {
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

        // Start first Actor w/ counter
        final ConcurrentMap<String, AtomicInteger> firstCounts = new ConcurrentHashMap<String, AtomicInteger>();
        NettyHelixActor<String> firstActor = new NettyHelixActor<String>(firstNode, firstPort, CODEC);
        firstActor.register(new HelixActorCallback<String>() {
            @Override
            public void onMessage(HelixActorScope scope, UUID messageId, String message) {
                String key = scope.getPartition() + ":" + scope.getState();
                firstCounts.putIfAbsent(key, new AtomicInteger());
                firstCounts.get(key).incrementAndGet();
            }
        });
        firstActor.start();

        // Start second Actor w/ counter
        final ConcurrentMap<String, AtomicInteger> secondCounts = new ConcurrentHashMap<String, AtomicInteger>();
        NettyHelixActor<String> secondActor = new NettyHelixActor<String>(secondNode, secondPort, CODEC);
        secondActor.register(new HelixActorCallback<String>() {
            @Override
            public void onMessage(HelixActorScope scope, UUID messageId, String message) {
                String key = scope.getPartition() + ":" + scope.getState();
                secondCounts.putIfAbsent(key, new AtomicInteger());
                secondCounts.get(key).incrementAndGet();
            }
        });
        secondActor.start();

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
                firstActor.send(RESOURCE_NAME, partitionName, "ONLINE", UUID.randomUUID(), "Hello world! " + i);
            }
        }

        // Loopback
        for (String partitionName : secondPartitions) {
            for (int i = 0; i < numMessages; i++) {
                secondActor.send(RESOURCE_NAME, partitionName, "ONLINE", UUID.randomUUID(), "Hello world! " + i);
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
