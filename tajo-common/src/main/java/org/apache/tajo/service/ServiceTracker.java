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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public interface ServiceTracker {

  public abstract boolean isHighAvailable();

  public abstract InetSocketAddress getUmbilicalAddress() throws ServiceTrackerException;

  public abstract InetSocketAddress getClientServiceAddress() throws ServiceTrackerException;

  public abstract InetSocketAddress getResourceTrackerAddress() throws ServiceTrackerException;

  public abstract InetSocketAddress getCatalogAddress() throws ServiceTrackerException;

  public abstract InetSocketAddress getMasterHttpInfo() throws ServiceTrackerException;

  /**
   * Add master name to shared storage.
   */
  public void register() throws IOException;


  /**
   * Delete master name to shared storage.
   *
   */
  public void delete() throws IOException;

  /**
   *
   * @return True if current master is an active master.
   */
  public boolean isActiveStatus();

  /**
   *
   * @return return all master list
   * @throws IOException
   */
  public List<TajoMasterInfo> getMasters() throws IOException;
}
