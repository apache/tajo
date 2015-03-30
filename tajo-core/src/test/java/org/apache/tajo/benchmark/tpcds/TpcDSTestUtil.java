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

package org.apache.tajo.benchmark.tpcds;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;

public class TpcDSTestUtil {
  private static final Log LOG = LogFactory.getLog(TpcDSTestUtil.class);

  static String [] tableNames = {
      "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
      "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band",
      "inventory", "item", "promotion", "reason", "ship_mode",
      "store", "store_returns", "store_sales", "time_dim", "warehouse",
      "web_page", "web_returns", "web_sales", "web_site"
  };

  private static String getDataDir() {
    String dataDir = System.getProperty("tpcds.data.dir");

    return dataDir;
  }

  public static void createTables(String database, TajoClient client) throws Exception {
    String dataDir = getDataDir();
    if (dataDir != null && dataDir.isEmpty()) {
      throw new IOException("No tpcds.data.dir property. Use -Dtpcds.data.dir=<data dir>");
    }
    File dataDirFile = new File(dataDir);
    if (!dataDirFile.exists()) {
      throw new IOException("tpcds.data.dir [" + dataDir + "] not exists.");
    }
    if (dataDirFile.isFile()) {
      throw new IOException("tpcds.data.dir [" + dataDir + "] is not a directory.");
    }

    for (String eachTable: tableNames) {
      File tableDataDir = new File(dataDirFile, eachTable);
      if (!tableDataDir.exists()) {
        throw new IOException(eachTable + " data dir [" + tableDataDir + "] not exists.");
      }
    }

    KeyValueSet opt = new KeyValueSet();
    opt.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);

    client.executeQuery("create database if not exists " + database);

    Path tpcdsResourceURL = new Path(ClassLoader.getSystemResource("tpcds").toString());

    Path ddlPath = new Path(tpcdsResourceURL, "ddl");
    FileSystem localFs = FileSystem.getLocal(new Configuration());

    FileStatus[] files = localFs.listStatus(ddlPath);

    String dataDirWithPrefix = dataDir;
    if (dataDir.indexOf("://") < 0) {
      dataDirWithPrefix = "file://" + dataDir;
    }
    for (FileStatus eachFile: files) {
      if (eachFile.isFile()) {
        String tableName = eachFile.getPath().getName().split("\\.")[0];
        String query = FileUtil.readTextFile(new File(eachFile.getPath().toUri()));
        query = query.replace("${DB}", database);
        query = query.replace("${DATA_LOCATION}", dataDirWithPrefix + "/" + tableName);

        LOG.info("Create table:" + tableName + "," + query);
        client.executeQuery(query);
      }
    }
  }
}

