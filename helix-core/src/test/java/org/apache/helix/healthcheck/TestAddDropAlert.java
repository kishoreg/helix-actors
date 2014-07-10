package org.apache.helix.healthcheck;

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

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockEspressoHealthReportProvider;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAddDropAlert extends ZkIntegrationTestBase {
  protected ClusterSetup _setupTool = null;
  protected final String _alertStr =
      "EXP(accumulate()(localhost_12918.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(10)";
  protected final String _alertStatusStr = _alertStr; // +" : (*)";
  protected final String _dbName = "TestDB0";

  @BeforeClass()
  public void beforeClass() throws Exception {
    _setupTool = new ClusterSetup(_gZkClient);
  }

  @AfterClass
  public void afterClass() {
  }

  public class AddDropAlertTransition extends MockTransition {
    @Override
    public void doTransition(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      String fromState = message.getFromState();
      String toState = message.getToState();
      String instance = message.getTgtName();
      String partition = message.getPartitionName();

      if (fromState.equalsIgnoreCase("SLAVE") && toState.equalsIgnoreCase("MASTER")) {

        // add a stat and report to ZK
        // perhaps should keep reporter per instance...
        ParticipantHealthReportCollectorImpl reporter =
            new ParticipantHealthReportCollectorImpl(manager, instance);
        MockEspressoHealthReportProvider provider = new MockEspressoHealthReportProvider();
        reporter.addHealthReportProvider(provider);
        String statName = "latency";
        provider.setStat(_dbName, statName, "15");
        reporter.transmitHealthReports();

        // sleep long enough for first set of alerts to report and alert to get deleted
        // then change reported data
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          System.err.println("Error sleeping");
        }
        provider.setStat(_dbName, statName, "1");
        reporter.transmitHealthReports();

        /*
         * for (int i = 0; i < 5; i++) { accessor.setProperty(PropertyType.HEALTHREPORT,
         * new ZNRecord("mockAlerts" + i), instance, "mockAlerts"); try {
         * Thread.sleep(1000); } catch (InterruptedException e) { // TODO Auto-generated
         * catch block e.printStackTrace(); } }
         */
      }
    }
  }

  @Test()
  public void testAddDropAlert() throws Exception {
    String clusterName = getShortClassName();
    MockParticipantManager[] participants = new MockParticipantManager[5];

    System.out.println("START TestAddDropAlert at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource group
        5, // number of nodes //change back to 5!!!
        1, // replicas //change back to 3!!!
        "MasterSlave", true); // do rebalance
    // enableHealthCheck(clusterName);

    _setupTool.getClusterManagementTool().addAlert(clusterName, _alertStr);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) // !!!change back to 5
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].setTransition(new AddDropAlertTransition());
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // drop alert soon after adding, but leave enough time for alert to fire once
    // Thread.sleep(3000);
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    new HealthStatsAggregator(controller).aggregate();
    String instance = "localhost_12918";
    ZNRecord record = accessor.getProperty(keyBuilder.alertStatus()).getRecord();
    Map<String, Map<String, String>> recMap = record.getMapFields();
    Set<String> keySet = recMap.keySet();
    Assert.assertTrue(keySet.size() > 0);

    _setupTool.getClusterManagementTool().dropAlert(clusterName, _alertStr);
    new HealthStatsAggregator(controller).aggregate();
    // other verifications go here
    // for (int i = 0; i < 1; i++) //change 1 back to 5
    // {
    // String instance = "localhost_" + (12918 + i);
    record = accessor.getProperty(keyBuilder.alertStatus()).getRecord();
    recMap = record.getMapFields();
    keySet = recMap.keySet();
    Assert.assertEquals(keySet.size(), 0);
    // }

    System.out.println("END TestAddDropAlert at " + new Date(System.currentTimeMillis()));
  }
}
