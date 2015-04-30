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

package org.apache.tajo.catalog;

import com.google.protobuf.ServiceException;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService.BlockingInterface;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.RpcConstants;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public class CatalogClient extends AbstractCatalogClient {
  protected NettyClientBase client;
  protected ServiceTracker serviceTracker;
  protected InetSocketAddress catalogServerAddr;
  /**
   * @throws java.io.IOException
   *
   */
  public CatalogClient(final TajoConf conf) throws IOException {
    super(conf);
    this.catalogServerAddr = NetUtils.createSocketAddr(conf.getVar(ConfVars.CATALOG_ADDRESS));
    this.serviceTracker = ServiceTrackerFactory.get(conf);
  }


  @Override
  BlockingInterface getStub() throws ServiceException {
    return getCatalogConnection().getStub();
  }

  private InetSocketAddress getCatalogServerAddr() {
    if (!conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      return catalogServerAddr;
    } else {
      return serviceTracker.getCatalogAddress();
    }
  }

  public synchronized NettyClientBase getCatalogConnection() throws ServiceException {
    if (client != null && client.isConnected()) return client;
    else {
      try {
        if (client != null && client.isConnected()) return client;
        RpcClientManager.cleanup(client);

        int retry = conf.getInt(RpcConstants.RPC_CLIENT_RETRY_MAX, RpcConstants.DEFAULT_RPC_RETRIES);
        // Client do not closed on idle state for support high available
        this.client = RpcClientManager.getInstance().newClient(getCatalogServerAddr(), CatalogProtocol.class, false,
            retry, 0, TimeUnit.SECONDS, false);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      return client;
    }
  }

  @Override
  public void close() {
    RpcClientManager.cleanup(client);
  }
}