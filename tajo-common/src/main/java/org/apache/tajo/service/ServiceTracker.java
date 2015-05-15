/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.service;

import org.apache.tajo.conf.TajoConf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public interface ServiceTracker {

  boolean isHighAvailable();

  InetSocketAddress getUmbilicalAddress() throws ServiceTrackerException;

  InetSocketAddress getClientServiceAddress() throws ServiceTrackerException;

  InetSocketAddress getResourceTrackerAddress() throws ServiceTrackerException;

  InetSocketAddress getCatalogAddress() throws ServiceTrackerException;

  InetSocketAddress getMasterHttpInfo() throws ServiceTrackerException;

  int getState(String masterName, TajoConf conf) throws ServiceTrackerException;

  int formatHA(TajoConf conf) throws ServiceTrackerException;

  List<String> getMasters(TajoConf conf) throws ServiceTrackerException;

  /**
   * Add master name to shared storage.
   */
  void register() throws IOException;


  /**
   * Delete master name to shared storage.
   *
   */
  void delete() throws IOException;

  /**
   *
   * @return True if current master is an active master.
   */
  boolean isActiveMaster();

  /**
   *
   * @return return all master list
   * @throws IOException
   */
  List<TajoMasterInfo> getMasters() throws IOException;
}
