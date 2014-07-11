package org.apache.helix.actor;

import org.apache.helix.*;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestNettyHelixActor extends ZkIntegrationTestBase {

    private static final String CLUSTER_NAME = "TEST_CLUSTER";
    private static final String RESOURCE_NAME = "MyResource";
    private static final HelixActorMessageCodec<String> CODEC = new HelixActorMessageCodec<String>() {
        @Override
        public byte[] encode(String message) {
            return message.getBytes();
        }

        @Override
        public String decode(byte[] message) {
            return new String(message);
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
        clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, CLUSTER_NAME + ",localhost_" + firstPort, "ACTOR_PORT=" + firstPort);
        clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, CLUSTER_NAME + ",localhost_" + secondPort, "ACTOR_PORT=" + secondPort);

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
    }

    @AfterClass
    public void afterClass() throws Exception {
        firstNode.disconnect();
        secondNode.disconnect();
        controller.disconnect();
    }

    @Test
    public void testMessagePassing() throws Exception {
        // Start first Actor w/ counter
        final ConcurrentMap<String, AtomicInteger> firstCounts = new ConcurrentHashMap<String, AtomicInteger>();
        NettyHelixActor<String> firstActor = new NettyHelixActor<String>(firstNode, firstPort, CODEC);
        firstActor.register(RESOURCE_NAME, new HelixActorCallback<String>() {
            @Override
            public void onMessage(Partition partition, String state, String message) {
                String key = partition.getPartitionName() + ":" + state;
                firstCounts.putIfAbsent(key, new AtomicInteger());
                firstCounts.get(key).incrementAndGet();
            }
        });
        firstNode.addExternalViewChangeListener(firstActor);
        firstActor.start();

        // Start second Actor w/ counter
        final ConcurrentMap<String, AtomicInteger> secondCounts = new ConcurrentHashMap<String, AtomicInteger>();
        NettyHelixActor<String> secondActor = new NettyHelixActor<String>(secondNode, secondPort, CODEC);
        secondActor.register(RESOURCE_NAME, new HelixActorCallback<String>() {
            @Override
            public void onMessage(Partition partition, String state, String message) {
                String key = partition.getPartitionName() + ":" + state;
                secondCounts.putIfAbsent(key, new AtomicInteger());
                secondCounts.get(key).incrementAndGet();
            }
        });
        secondNode.addExternalViewChangeListener(secondActor);
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
            firstActor.send(new Partition(partitionName), "ONLINE", "Hello world!");
        }

        // Loopback
        for (String partitionName : secondPartitions) {
            secondActor.send(new Partition(partitionName), "ONLINE", "Hello world!");
        }

        // Check
        Thread.sleep(500);
        for (String partitionName : secondPartitions) {
            AtomicInteger count = secondCounts.get(partitionName + ":ONLINE");
            Assert.assertNotNull(count);
            Assert.assertEquals(count.get(), 2);
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
