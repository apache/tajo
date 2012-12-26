/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package tajo.catalog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.conf.TajoConf;

import java.io.IOException;

/**
 * This class provides a catalog service interface in
 * local.
 *
 * @author Hyunsik Choi
 *
 */
public class LocalCatalog extends AbstractCatalogClient {
  private static final Log LOG = LogFactory.getLog(LocalCatalog.class);
  private CatalogServer catalog;

  public LocalCatalog(final TajoConf conf) throws IOException {
    this.catalog = new CatalogServer();
    this.catalog.init(conf);
    this.catalog.start();
    setStub(catalog.getHandler());
  }

  public LocalCatalog(final CatalogServer server) {
    this.catalog = server;
    setStub(server.getHandler());
  }

  public void shutdown() {
    this.catalog.stop();
  }
}
