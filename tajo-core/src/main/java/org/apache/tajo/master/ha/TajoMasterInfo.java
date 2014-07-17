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

package org.apache.tajo.master.ha;

public class TajoMasterInfo {

  private boolean available;
  private boolean isActive;

  private String rpcServerAddress;
  private String rpcClientAddress;
  private String resourceTrackerAddress;
  private String catalogAddress;
  private String webServerAddress;

  public String getRpcServerAddress() {
    return rpcServerAddress;
  }

  public void setRpcServerAddress(String rpcServerAddress) {
    this.rpcServerAddress = rpcServerAddress;
  }

  public String getRpcClientAddress() {
    return rpcClientAddress;
  }

  public void setRpcClientAddress(String rpcClientAddress) {
    this.rpcClientAddress = rpcClientAddress;
  }

  public String getResourceTrackerAddress() {
    return resourceTrackerAddress;
  }

  public void setResourceTrackerAddress(String resourceTrackerAddress) {
    this.resourceTrackerAddress = resourceTrackerAddress;
  }

  public String getCatalogAddress() {
    return catalogAddress;
  }

  public void setCatalogAddress(String catalogAddress) {
    this.catalogAddress = catalogAddress;
  }

  public String getWebServerAddress() {
    return webServerAddress;
  }

  public void setWebServerAddress(String webServerAddress) {
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
