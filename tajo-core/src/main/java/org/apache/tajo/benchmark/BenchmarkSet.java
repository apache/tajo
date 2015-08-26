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

package org.apache.tajo.benchmark;

import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.store.DerbyStore;
import org.apache.tajo.client.DummyServiceTracker;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class BenchmarkSet {
  protected TajoClient tajo;
  protected Map<String, Schema> schemas = new HashMap<String, Schema>();
  protected Map<String, Schema> outSchemas = new HashMap<String, Schema>();
  protected Map<String, String> queries = new HashMap<String, String>();
  protected String dataDir;

  public void init(TajoConf conf, String dataDir) throws SQLException {
    this.dataDir = dataDir;

    if (System.getProperty(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS.varname) != null) {

      String addressStr = System.getProperty(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS.varname);
      InetSocketAddress addr = NetUtils.createSocketAddr(addressStr);
      ServiceTracker serviceTracker = new DummyServiceTracker(addr);
      tajo = new TajoClientImpl(serviceTracker, null);

    } else {
      conf.set(CatalogConstants.STORE_CLASS, DerbyStore.class.getCanonicalName());
      tajo = new TajoClientImpl(ServiceTrackerFactory.get(conf));
    }
  }

  protected void loadQueries(String dir) throws IOException {
    // TODO - this code dead??
    File queryDir = new File(dir);

    if(!queryDir.exists()) {
      queryDir = new File(System.getProperty("user.dir") + "/tajo-core/" + dir);
    }

    if(!queryDir.exists())
    {
      return;
    }
    int last;
    String name, query;
    for (String file : queryDir.list()) {
      if (file.endsWith(".sql")) {
        last = file.indexOf(".sql");
        name = file.substring(0, last);
        query = FileUtil.readTextFile(new File(queryDir + "/" + file));
      }
    }
  }

  public abstract void loadSchemas();

  public abstract void loadOutSchema();

  public abstract void loadQueries() throws IOException;

  public abstract void loadTables() throws TajoException;

  public String [] getTableNames() {
    return schemas.keySet().toArray(new String[schemas.size()]);
  }

  public String getQuery(String queryName) {
    return queries.get(queryName);
  }

  public Schema getSchema(String name) {
    return schemas.get(name);
  }

  public Iterable<Schema> getSchemas() {
    return schemas.values();
  }

  public Schema getOutSchema(String name) {
    return outSchemas.get(name);
  }

  public void perform(String queryName) throws SQLException {
    String query = getQuery(queryName);
    if (query == null) {
      throw new IllegalArgumentException("#{queryName} does not exists");
    }
    long start = System.currentTimeMillis();
    tajo.executeQuery(query);
    long end = System.currentTimeMillis();

    System.out.println("====================================");
    System.out.println("QueryName: " + queryName);
    System.out.println("Query: " + query);
    System.out.println("Processing Time: " + (end - start));
    System.out.println("====================================");
  }
}
