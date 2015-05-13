/*
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

package org.apache.tajo.client;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerException;
import org.apache.tajo.service.TajoMasterInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class DummyServiceTracker implements ServiceTracker {
  private InetSocketAddress address;

  public DummyServiceTracker(InetSocketAddress address) {
    this.address = address;
  }

  @Override
  public boolean isHighAvailable() {
    return false;
  }

  @Override
  public InetSocketAddress getUmbilicalAddress() {
    throw new UnsupportedException();
  }

  @Override
  public InetSocketAddress getClientServiceAddress() {
    return address;
  }

  @Override
  public InetSocketAddress getResourceTrackerAddress() {
    throw new UnsupportedException();
  }

  @Override
  public InetSocketAddress getCatalogAddress() {
    throw new UnsupportedException();
  }

  @Override
  public InetSocketAddress getMasterHttpInfo() throws ServiceTrackerException {
    throw new UnsupportedException();
  }

  @Override
  public int getState(String masterName, TajoConf conf) throws ServiceTrackerException {
    return 0;
  }

  @Override
  public int formatHA(TajoConf conf) throws ServiceTrackerException {
    return 0;
  }

  @Override
  public List<String> getMasters(TajoConf conf) throws ServiceTrackerException {
    return new ArrayList<String>();
  }

  @Override
  public void register() throws IOException {
  }

  @Override
  public void delete() throws IOException {
  }

  @Override
  public boolean isActiveMaster() {
    return true;
  }

  @Override
  public List<TajoMasterInfo> getMasters() throws IOException {
    throw new UnsupportedException();
  }
}
