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

import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService.BlockingInterface;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public class CatalogClient extends AbstractCatalogClient {
  /**
   * @throws java.io.IOException
   *
   */
  public CatalogClient(final TajoConf conf) throws IOException {
    super(conf, NetUtils.createSocketAddr(conf.getVar(ConfVars.CATALOG_ADDRESS)));
  }

  public CatalogClient(TajoConf conf, String host, int port) throws IOException {
    super(conf, NetUtils.createSocketAddr(host, port));
  }

  @Override
  BlockingInterface getStub(NettyClientBase client) {
    return client.getStub();
  }

  public void close() {
  }
}