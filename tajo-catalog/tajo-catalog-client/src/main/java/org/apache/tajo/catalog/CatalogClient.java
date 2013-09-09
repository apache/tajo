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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService.BlockingInterface;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.ProtoBlockingRpcClient;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public class CatalogClient extends AbstractCatalogClient {
  private final Log LOG = LogFactory.getLog(CatalogClient.class);
  private ProtoBlockingRpcClient client;

  /**
   * @throws java.io.IOException
   *
   */
  public CatalogClient(final TajoConf conf) throws IOException {
    String catalogAddr = conf.getVar(ConfVars.CATALOG_ADDRESS);
    connect(NetUtils.createSocketAddr(catalogAddr));
  }

  public CatalogClient(String host, int port) throws IOException {
    connect(NetUtils.createSocketAddr(host, port));
  }

  private void connect(InetSocketAddress serverAddr) throws IOException {
    String addrStr = NetUtils.normalizeInetSocketAddress(serverAddr);
    LOG.info("Trying to connect the catalog (" + addrStr + ")");
    try {
      client = new ProtoBlockingRpcClient(CatalogProtocol.class, serverAddr);
      setStub((BlockingInterface) client.getStub());
    } catch (Exception e) {
      throw new IOException("Can't connect the catalog server (" + addrStr +")");
    }
    LOG.info("Connected to the catalog server (" + addrStr + ")");
  }

  public void close() {
    client.close();
  }
}