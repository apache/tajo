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

package org.apache.tajo;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.ResultSet;
import java.util.Map;

public class TpchTestBase {
  private static final Log LOG = LogFactory.getLog(TpchTestBase.class);

  String [] names;
  String [] paths;
  Schema[] schemas;
  Map<String, Integer> nameMap = Maps.newHashMap();
  Map<String, PartitionMethodDesc> partitionMap = Maps.newHashMap();
  protected TPCH tpch;
  protected LocalTajoTestingUtility util;

  private static TpchTestBase testBase;

  static {
    try {
      testBase = new TpchTestBase();
      testBase.setUp();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }


  private TpchTestBase() throws IOException, URISyntaxException {
    names = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier", "empty_orders"};
    paths = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      nameMap.put(names[i], i);
    }

    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadQueries();

    schemas = new Schema[names.length];
    for (int i = 0; i < names.length; i++) {
      schemas[i] = tpch.getSchema(names[i]);

      if (TPCH.PARTITION_TABLE != null && TPCH.PARTITION_TABLE.equals("true")) {
        if (tpch.getPartitionMethodDesc(names[i]) != null) {
          partitionMap.put(names[i], tpch.getPartitionMethodDesc(names[i]));
        }
      }
    }

    // create a temporal table
    File tpchTablesDir = new File(new File(CommonTestingUtil.getTestDir().toUri()), "tpch");

    for (int i = 0; i < names.length; i++) {
      String str = null;
      Path tablePath = null;

      if (partitionMap.get(names[i]) != null) {
        URL url = FileUtil.getResourcePath("tpch_partition/" + names[i] + "/");
        Path resourcePath = new Path(url.toURI());
        FileSystem fs = resourcePath.getFileSystem(new TajoConf());

        tablePath = new Path(new Path(tpchTablesDir.toURI()), names[i]);
        fs.mkdirs(tablePath);

        FileStatus[] partitionStatuses = fs.listStatus(resourcePath);

        for(FileStatus partitionStatus : partitionStatuses) {
          Path partitionPath = new Path(tablePath, partitionStatus.getPath().getName());
          fs.mkdirs(partitionPath);

          str = FileUtil.readTextFileFromResource("tpch_partition/" + names[i]
            + "/" + partitionStatus.getPath().getName() + "/" + names[i]  + ".tbl");

          Path dataPath = new Path(partitionPath, names[i] + ".tbl");
          FileUtil.writeTextToFile(str, dataPath);
        }
      } else {
        str = FileUtil.readTextFileFromResource("tpch/" + names[i] + ".tbl");
        tablePath = new Path(new Path(tpchTablesDir.toURI()), names[i] + ".tbl");
        FileUtil.writeTextToFile(str, tablePath);
      }
      paths[i] = tablePath.toString();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void setUp() throws Exception {
    util = new LocalTajoTestingUtility();
    KeyValueSet opt = new KeyValueSet();
    opt.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    util.setup(names, paths, schemas, opt, partitionMap);
  }

  public static TpchTestBase getInstance() {
    return testBase;
  }

  public ResultSet execute(String query) throws Exception {
    return util.execute(query);
  }

  public TajoTestingCluster getTestingCluster() {
    return util.getTestingCluster();
  }

  public String getPath(String tableName) {
    if (!nameMap.containsKey(tableName)) {
      throw new RuntimeException("No such a table name '" + tableName + "'");
    }
    return paths[nameMap.get(tableName)];
  }
}
