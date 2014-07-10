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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.ZkStandAloneCMTestBaseWithPropertyServerCheck;
import org.apache.helix.model.AlertHistory;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.HelixConfigScope;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class TestAlertFireHistory extends ZkStandAloneCMTestBaseWithPropertyServerCheck {
  private final static Logger LOG = Logger.getLogger(TestAlertFireHistory.class);

  String _statName = "TestStat@DB=db1";
  String _stat = "TestStat";
  String metricName1 = "TestMetric1";
  String metricName2 = "TestMetric2";

  String _alertStr1 = "EXP(decay(1.0)(localhost_*.TestStat@DB=db1.TestMetric1))CMP(GREATER)CON(20)";
  String _alertStr2 =
      "EXP(decay(1.0)(localhost_*.TestStat@DB=db1.TestMetric2))CMP(GREATER)CON(100)";

  void setHealthData(int[] val1, int[] val2) {
    for (int i = 0; i < NODE_NR; i++) {
      HelixManager manager = _participants[i];
      ZNRecord record = new ZNRecord(_stat);
      Map<String, String> valMap = new HashMap<String, String>();
      valMap.put(metricName1, val1[i] + "");
      valMap.put(metricName2, val2[i] + "");
      record.setSimpleField("TimeStamp", new Date().getTime() + "");
      record.setMapField(_statName, valMap);
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      Builder keyBuilder = helixDataAccessor.keyBuilder();
      helixDataAccessor.setProperty(
          keyBuilder.healthReport(manager.getInstanceName(), record.getId()),
          new HealthStat(record));
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOG.error("Interrupted sleep", e);
    }
  }

  @Test
  public void testAlertDisable() throws InterruptedException {

    int[] metrics1 = {
        10, 15, 22, 24, 16
    };
    int[] metrics2 = {
        22, 115, 22, 141, 16
    };
    setHealthData(metrics1, metrics2);

    HelixManager manager = _controller;
    manager.startTimerTasks();

    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr1);
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr2);

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(CLUSTER_NAME).build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("healthChange.enabled", "false");
    _setupTool.getClusterManagementTool().setConfig(scope, properties);

    HealthStatsAggregator task = new HealthStatsAggregator(_controller);

    task.aggregate();
    Thread.sleep(100);
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();

    AlertHistory history = manager.getHelixDataAccessor().getProperty(keyBuilder.alertHistory());

    Assert.assertEquals(history, null);

    properties.put("healthChange.enabled", "true");
    _setupTool.getClusterManagementTool().setConfig(scope, properties);

    task.aggregate();
    Thread.sleep(100);

    history = manager.getHelixDataAccessor().getProperty(keyBuilder.alertHistory());
    //
    Assert.assertNotNull(history);
    Assert.assertEquals(history.getRecord().getMapFields().size(), 1);
  }

  @Test
  public void testAlertHistory() throws InterruptedException {
    int[] metrics1 = {
        10, 15, 22, 24, 16
    };
    int[] metrics2 = {
        22, 115, 22, 141, 16
    };
    setHealthData(metrics1, metrics2);

    HelixManager manager = _controller;
    for (HelixTimerTask task : _controller.getControllerTimerTasks()) {
      task.stop();
    }

    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr1);
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr2);

    int historySize = 0;
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    HelixProperty property = helixDataAccessor.getProperty(keyBuilder.alertHistory());
    ZNRecord history = null;
    if (property != null) {
      history = property.getRecord();
      historySize = property.getRecord().getMapFields().size();
    }

    HealthStatsAggregator task = new HealthStatsAggregator(_controller);

    task.aggregate();
    Thread.sleep(100);

    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    //
    Assert.assertEquals(history.getMapFields().size(), 1 + historySize);
    TreeMap<String, Map<String, String>> recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll(history.getMapFields());
    Map<String, String> lastRecord = recordMap.firstEntry().getValue();
    Assert.assertTrue(lastRecord.size() == 4);
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("ON"));

    setHealthData(metrics1, metrics2);
    task.aggregate();
    Thread.sleep(100);
    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    // no change
    Assert.assertEquals(history.getMapFields().size(), 1 + historySize);
    recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll(history.getMapFields());
    lastRecord = recordMap.firstEntry().getValue();
    Assert.assertTrue(lastRecord.size() == 4);
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("ON"));

    int[] metrics3 = {
        21, 44, 22, 14, 16
    };
    int[] metrics4 = {
        122, 115, 222, 41, 16
    };
    setHealthData(metrics3, metrics4);
    task.aggregate();
    Thread.sleep(100);
    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    // new delta should be recorded
    Assert.assertEquals(history.getMapFields().size(), 2 + historySize);
    recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll(history.getMapFields());
    lastRecord = recordMap.lastEntry().getValue();
    Assert.assertEquals(lastRecord.size(), 6);
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("ON"));

    int[] metrics5 = {
        0, 0, 0, 0, 0
    };
    int[] metrics6 = {
        0, 0, 0, 0, 0
    };
    setHealthData(metrics5, metrics6);
    task.aggregate();

    for (int i = 0; i < 10; i++) {
      Thread.sleep(500);
      history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
      recordMap = new TreeMap<String, Map<String, String>>();
      recordMap.putAll(history.getMapFields());
      lastRecord = recordMap.lastEntry().getValue();

      if (history.getMapFields().size() == 3 + historySize && lastRecord.size() == 6) {
        break;
      }
    }

    // reset everything
    Assert.assertEquals(history.getMapFields().size(), 3 + historySize,
        "expect history-map-field size is " + (3 + historySize) + ", but was " + history);
    Assert
        .assertTrue(lastRecord.size() == 6, "expect last-record size is 6, but was " + lastRecord);

    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)")
        .equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)")
        .equals("OFF"));

    // Size of the history should be 30
    for (int i = 0; i < 27; i++) {
      int x = i % 2;
      int y = (i + 1) % 2;
      int[] metricsx = {
          19 + 3 * x, 19 + 3 * y, 19 + 4 * x, 18 + 4 * y, 17 + 5 * y
      };
      int[] metricsy = {
          99 + 3 * x, 99 + 3 * y, 98 + 4 * x, 98 + 4 * y, 97 + 5 * y
      };

      setHealthData(metricsx, metricsy);
      task.aggregate();
      Thread.sleep(100);
      history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();

      Assert.assertEquals(history.getMapFields().size(), Math.min(3 + i + 1 + historySize, 30));
      recordMap = new TreeMap<String, Map<String, String>>();
      recordMap.putAll(history.getMapFields());
      lastRecord = recordMap.lastEntry().getValue();
      if (i == 0) {
        Assert.assertTrue(lastRecord.size() == 6);
        Assert.assertTrue(lastRecord
            .get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
      } else {
        System.out.println(lastRecord.size());
        Assert.assertEquals(lastRecord.size(), 10);
        if (x == 0) {
          Assert.assertTrue(lastRecord.get(
              "(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        } else {
          Assert.assertTrue(lastRecord.get(
              "(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get(
              "(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        }
      }
    }
    // size limit is 30
    for (int i = 0; i < 10; i++) {
      int x = i % 2;
      int y = (i + 1) % 2;
      int[] metricsx = {
          19 + 3 * x, 19 + 3 * y, 19 + 4 * x, 18 + 4 * y, 17 + 5 * y
      };
      int[] metricsy = {
          99 + 3 * x, 99 + 3 * y, 98 + 4 * x, 98 + 4 * y, 97 + 5 * y
      };

      setHealthData(metricsx, metricsy);
      task.aggregate();
      for (int j = 0; j < 10; j++) {
        Thread.sleep(100);
        history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
        recordMap = new TreeMap<String, Map<String, String>>();
        recordMap.putAll(history.getMapFields());
        lastRecord = recordMap.lastEntry().getValue();

        if (history.getMapFields().size() == 30 && lastRecord.size() == 10)
          break;
      }
      Assert.assertEquals(history.getMapFields().size(), 30,
          "expect history.map-field size is 30, but was " + history);
      Assert.assertEquals(lastRecord.size(), 10, "expect last-record size is 10, but was "
          + lastRecord);

      if (x == 0) {
        Assert.assertTrue(lastRecord.get(
            "(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
      } else {
        Assert.assertTrue(lastRecord.get(
            "(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get(
            "(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord
            .get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
      }
    }

  }

}

