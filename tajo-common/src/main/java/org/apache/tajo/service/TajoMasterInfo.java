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

import java.net.InetSocketAddress;

public class TajoMasterInfo {

  private boolean available;
  private boolean isActive;

  private InetSocketAddress tajoMasterAddress;
  private InetSocketAddress tajoClientAddress;
  private InetSocketAddress workerResourceTrackerAddr;
  private InetSocketAddress catalogAddress;
  private InetSocketAddress webServerAddress;

  public InetSocketAddress getTajoMasterAddress() {
    return tajoMasterAddress;
  }

  public void setTajoMasterAddress(InetSocketAddress tajoMasterAddress) {
    this.tajoMasterAddress = tajoMasterAddress;
  }

  public InetSocketAddress getTajoClientAddress() {
    return tajoClientAddress;
  }

  public void setTajoClientAddress(InetSocketAddress tajoClientAddress) {
    this.tajoClientAddress = tajoClientAddress;
  }

  public InetSocketAddress getWorkerResourceTrackerAddr() {
    return workerResourceTrackerAddr;
  }

  public void setWorkerResourceTrackerAddr(InetSocketAddress workerResourceTrackerAddr) {
    this.workerResourceTrackerAddr = workerResourceTrackerAddr;
  }

  public InetSocketAddress getCatalogAddress() {
    return catalogAddress;
  }

  public void setCatalogAddress(InetSocketAddress catalogAddress) {
    this.catalogAddress = catalogAddress;
  }

  public InetSocketAddress getWebServerAddress() {
    return webServerAddress;
  }

  public void setWebServerAddress(InetSocketAddress webServerAddress) {
    this.webServerAddress = webServerAddress;
  }

  public boolean isAvailable() {
    return available;
  }

  public void setAvailable(boolean available) {
    this.available = available;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActive(boolean active) {
    isActive = active;
  }
}
