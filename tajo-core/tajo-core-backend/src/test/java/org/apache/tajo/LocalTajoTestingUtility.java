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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;
import java.sql.ResultSet;

public class LocalTajoTestingUtility {
  private TajoTestingCluster util;
  private TajoConf conf;
  private TajoClient client;

  public void setup(String[] names,
                    String[] tablepaths,
                    Schema[] schemas,
                    Options option) throws Exception {

    util = new TajoTestingCluster();
    util.startMiniCluster(1);
    conf = util.getConfiguration();
    client = new TajoClient(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMaster().
        getStorageManager().getBaseDir();
    fs.mkdirs(rootDir);
    for (int i = 0; i < tablepaths.length; i++) {
      Path localPath = new Path(tablepaths[i]);
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dataPath = new Path(tablePath, "data");
      fs.mkdirs(dataPath);
      Path dfsPath = new Path(dataPath, localPath.getName());
      fs.copyFromLocalFile(localPath, dfsPath);
      TableMeta meta = CatalogUtil.newTableMeta(schemas[i],
          CatalogProtos.StoreType.CSV, option);
      client.createTable(names[i], tablePath, meta);
    }
  }

  public TajoTestingCluster getTestingCluster() {
    return util;
  }

  public ResultSet execute(String query) throws IOException, ServiceException {
    return client.executeQueryAndGetResult(query);
  }

  public void shutdown() throws IOException {
    client.close();
    util.shutdownMiniCluster();
  }
}
