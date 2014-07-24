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

/**
 * An interface that resolves a message scope to a direct address.
 */
public interface HelixResolver {
  /**
   * Initialize a connection for scope resolution.
   */
  void connect();

  /**
   * Tear down any state and open connections to Helix clusters.
   */
  void disconnect();

  /**
   * Check the connection status
   * @return true if connected, false otherwise
   */
  boolean isConnected();

  /**
   * Resolve a scope.
   *
   * <p>
   *     After this is called, {@link HelixMessageScope#getDestinationAddresses()} for the provided scope
   *     will return a non-null value.
   * </p>
   */
  void resolve(HelixMessageScope scope);
}
