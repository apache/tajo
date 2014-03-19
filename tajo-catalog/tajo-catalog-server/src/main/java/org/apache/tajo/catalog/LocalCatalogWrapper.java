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

/**
 *
 */
package org.apache.tajo.catalog;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.rpc.NettyClientBase;

import java.io.IOException;

/**
 * This class provides a catalog service interface in
 * local.
 */
public class LocalCatalogWrapper extends AbstractCatalogClient {
  private CatalogServer catalog;
  private CatalogProtocol.CatalogProtocolService.BlockingInterface stub;

  public LocalCatalogWrapper(final TajoConf conf) throws IOException {
    super(conf, null);
    this.catalog = new CatalogServer();
    this.catalog.init(conf);
    this.catalog.start();
    this.stub = catalog.getHandler();
  }

  public LocalCatalogWrapper(final CatalogServer server) {
    this(server, server.getConf());
  }

  public LocalCatalogWrapper(final CatalogServer server, final TajoConf conf) {
    super(conf, null);
    this.catalog = server;
    this.stub = server.getHandler();
  }

  public void shutdown() {
    this.catalog.stop();
  }

  @Override
  CatalogProtocol.CatalogProtocolService.BlockingInterface getStub(NettyClientBase client) {
    return stub;
  }
}
