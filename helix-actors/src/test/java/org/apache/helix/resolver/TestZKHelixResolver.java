package org.apache.helix.resolver;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.resolver.zk.ZKHelixResolver;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * Test basic routing table lookups for a ZK-based Helix resolver.
 */
public class TestZKHelixResolver extends ZkUnitTestBase {
  private static final int NUM_PARTICIPANTS = 2;
  private static final int NUM_PARTITIONS = 2;
  private static final String CLUSTER_NAME = TestZKHelixResolver.class.getSimpleName();
  private static final String RESOURCE_NAME = "MyResource";
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private ClusterSetup _setupTool;
  private HelixResolver _resolver;
  private Map<String, InetSocketAddress> _socketMap;

  @BeforeClass
  public void beforeClass() {
    // Set up cluster
    _setupTool = new ClusterSetup(_gZkClient);
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, "OnlineOffline",
        IdealState.RebalanceMode.FULL_AUTO.toString());
    _setupTool.rebalanceCluster(CLUSTER_NAME, RESOURCE_NAME, 1, RESOURCE_NAME, null);

    // Set up and start instances
    _socketMap = Maps.newHashMap();
    HelixAdmin admin = _setupTool.getClusterManagementTool();
    _participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String host = "localhost";
      int port = i;
      int ipcPort = i + 100;
      String instanceName = host + "_" + port;
      InstanceConfig config = new InstanceConfig(instanceName);
      config.setHostName(host);
      config.setPort(Integer.toString(port));
      config.getRecord().setSimpleField("IPC_PORT", Integer.toString(ipcPort));
      admin.addInstance(CLUSTER_NAME, config);
      _socketMap.put(instanceName, new InetSocketAddress(host, ipcPort));
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    // Start controller
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "controller_0");
    _controller.syncStart();

    // Connect a resolver
    _resolver = new ZKHelixResolver(ZK_ADDR);
    _resolver.connect();

    // Wait for External view convergence
    ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
        ZK_ADDR, CLUSTER_NAME), 10000);
  }

  @Test
  public void testResolution() {
    HelixMessageScope clusterScope = new HelixMessageScope.Builder().cluster(CLUSTER_NAME).build();
    _resolver.resolve(clusterScope);
    Assert.assertNotNull(clusterScope.getDestinationAddresses());
    Assert.assertTrue(clusterScope.getDestinationAddresses().values().containsAll(_socketMap.values()), "Expected "
        + _socketMap.values() + ", found " + clusterScope.getDestinationAddresses());

    HelixMessageScope resourceScope =
        new HelixMessageScope.Builder().cluster(CLUSTER_NAME).resource(RESOURCE_NAME).build();
    _resolver.resolve(resourceScope);
    Assert.assertNotNull(resourceScope.getDestinationAddresses());
    Assert.assertTrue(resourceScope.getDestinationAddresses().values().containsAll(_socketMap.values()), "Expected "
        + _socketMap.values() + ", found " + resourceScope.getDestinationAddresses());

    HelixMessageScope partition0Scope =
        new HelixMessageScope.Builder().cluster(CLUSTER_NAME).resource(RESOURCE_NAME)
            .partition(RESOURCE_NAME + "_0").build();
    _resolver.resolve(partition0Scope);
    Assert.assertNotNull(partition0Scope.getDestinationAddresses());
    ExternalView externalView =
        _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME);
    Set<String> instanceSet = externalView.getStateMap(RESOURCE_NAME + "_0").keySet();
    Set<InetSocketAddress> expectedSocketAddrs = Sets.newHashSet();
    for (String instanceName : instanceSet) {
      expectedSocketAddrs.add(_socketMap.get(instanceName));
    }
    Assert.assertEquals(partition0Scope.getDestinationAddresses().values(), expectedSocketAddrs,
            "Expected " + expectedSocketAddrs + ", found " + partition0Scope.getDestinationAddresses().values());

    HelixMessageScope sourceInstanceScope = new HelixMessageScope.Builder()
            .cluster(CLUSTER_NAME)
            .resource(RESOURCE_NAME)
            .partition(RESOURCE_NAME + "_0")
            .sourceInstance(_participants[0].getInstanceName())
            .build();
    _resolver.resolve(sourceInstanceScope);
    Assert.assertNotNull(sourceInstanceScope.getSourceAddress());
    Assert.assertEquals(sourceInstanceScope.getSourceAddress(), _socketMap.get(_participants[0].getInstanceName()));
  }

  @AfterClass
  public void afterClass() {
    _resolver.disconnect();
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
  }

}
