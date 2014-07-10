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

import org.apache.helix.monitoring.SensorNameProvider;

public interface ClusterStatusMonitorMBean extends SensorNameProvider {
  public long getDownInstanceGauge();

  public long getInstancesGauge();

  public long getDisabledInstancesGauge();

  public long getDisabledPartitionsGauge();

  /**
   * The max message queue size across all instances including controller
   * will report to ingraph
   * @return
   */
  public long getMaxMessageQueueSizeGauge();

  /**
   * Get all message queue sizes as a string
   * will NOT report to ingraph
   * @return
   */
  public String getMessageQueueSizes();
}
