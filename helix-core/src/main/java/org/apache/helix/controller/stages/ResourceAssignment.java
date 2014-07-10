package org.apache.helix.controller.stages;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.Partition;

/**
 * Represents the assignments of replicas for an entire resource, keyed on partitions of the
 * resource. Each partition has its replicas assigned to a node, and each replica is in a state.
 */
public class ResourceAssignment {

  private final Map<Partition, Map<String, String>> _resourceMap;

  public ResourceAssignment() {
    this(new HashMap<Partition, Map<String, String>>());
  }

  public ResourceAssignment(Map<Partition, Map<String, String>> resourceMap) {
    _resourceMap = resourceMap;
  }

  public Map<Partition, Map<String, String>> getResourceMap() {
    return _resourceMap;
  }

  public Map<String, String> getInstanceStateMap(Partition partition) {
    if (_resourceMap.containsKey(partition)) {
      return _resourceMap.get(partition);
    }
    return Collections.emptyMap();
  }

  public void addReplicaMap(Partition partition, Map<String, String> replicaMap) {
    _resourceMap.put(partition, replicaMap);
  }
}
