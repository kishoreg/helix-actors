package org.apache.helix.monitoring.mbeans;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.Mocks;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestResourceMonitor {
  String _clusterName = "Test-cluster";
  String _dbName = "TestDB";
  int _replicas = 3;
  int _partitions = 50;

  class MockHelixManager extends Mocks.MockManager {
    class MockDataAccessor extends Mocks.MockAccessor {
      @Override
      public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) {
        List<T> result = new ArrayList<T>();
        PropertyType type = key.getType();
        Class<? extends HelixProperty> clazz = key.getTypeClass();
        if (type == PropertyType.EXTERNALVIEW) {
          HelixProperty typedInstance = HelixProperty.convertToTypedInstance(clazz, _externalView);
          result.add((T) typedInstance);
          return result;
        } else if (type == PropertyType.LIVEINSTANCES) {
          return (List<T>) HelixProperty.convertToTypedList(clazz, _liveInstances);
        }

        return result;
      }

      @Override
      public <T extends HelixProperty> T getProperty(PropertyKey key) {
        PropertyType type = key.getType();
        if (type == PropertyType.EXTERNALVIEW) {
          return (T) new ExternalView(_externalView);
        } else if (type == PropertyType.IDEALSTATES) {
          return (T) new IdealState(_idealState);
        }
        return null;
      }
    }

    HelixDataAccessor _accessor = new MockDataAccessor();
    ZNRecord _idealState;
    ZNRecord _externalView;
    List<String> _instances;
    List<ZNRecord> _liveInstances;
    String _db = "DB";

    public MockHelixManager() {
      _liveInstances = new ArrayList<ZNRecord>();
      _instances = new ArrayList<String>();
      for (int i = 0; i < 5; i++) {
        String instance = "localhost_" + (12918 + i);
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), UUID.randomUUID()
            .toString());

      }
      _idealState =
          DefaultIdealStateCalculator.calculateIdealState(_instances, _partitions, _replicas,
              _dbName, "MASTER", "SLAVE");
      _externalView = new ZNRecord(_idealState);
    }

    @Override
    public HelixDataAccessor getHelixDataAccessor() {
      return _accessor;
    }

  }

  @Test()
  public void TestReportData() {
    MockHelixManager manager = new MockHelixManager();
    ResourceMonitor monitor = new ResourceMonitor(_clusterName, _dbName);

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    ExternalView externalView = helixDataAccessor.getProperty(keyBuilder.externalView(_dbName));
    IdealState idealState = helixDataAccessor.getProperty(keyBuilder.idealStates(_dbName));

    monitor.updateExternalView(externalView, idealState);

    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
    AssertJUnit.assertEquals(monitor.getErrorPartitionGauge(), 0);
    AssertJUnit.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
    AssertJUnit.assertEquals(monitor.getPartitionGauge(), _partitions);
    monitor.getBeanName();

    int n = 4;
    for (int i = 0; i < n; i++) {
      Map<String, String> map = externalView.getStateMap(_dbName + "_" + 3 * i);
      String key = map.keySet().toArray()[0].toString();
      map.put(key, "ERROR");
      externalView.setStateMap(_dbName + "_" + 3 * i, map);
    }

    monitor.updateExternalView(externalView, idealState);
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
    AssertJUnit.assertEquals(monitor.getErrorPartitionGauge(), n);
    AssertJUnit.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
    AssertJUnit.assertEquals(monitor.getPartitionGauge(), _partitions);

    n = 5;
    for (int i = 0; i < n; i++) {
      externalView.getRecord().getMapFields().remove(_dbName + "_" + 4 * i);
    }

    monitor.updateExternalView(externalView, idealState);
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), n * (_replicas + 1));
    AssertJUnit.assertEquals(monitor.getErrorPartitionGauge(), 3);
    AssertJUnit.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions - n);
    AssertJUnit.assertEquals(monitor.getPartitionGauge(), _partitions);
  }
}
