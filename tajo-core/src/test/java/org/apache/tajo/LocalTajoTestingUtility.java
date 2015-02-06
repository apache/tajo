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

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TajoIdUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.util.UUID;

public class LocalTajoTestingUtility {
  private static final Log LOG = LogFactory.getLog(LocalTajoTestingUtility.class);

  private TajoTestingCluster util;
  private TajoConf conf;
  private TajoClient client;

  private static UserGroupInformation dummyUserInfo;

  static {
    try {
      dummyUserInfo = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static int taskAttemptId;

  public static TaskAttemptId newTaskAttemptId() {
    return QueryIdFactory.newTaskAttemptId(
        QueryIdFactory.newTaskId(new MasterPlan(newQueryId(), null, null).newExecutionBlockId()), taskAttemptId++);
  }
  public static TaskAttemptId newTaskAttemptId(MasterPlan plan) {
    return QueryIdFactory.newTaskAttemptId(QueryIdFactory.newTaskId(plan.newExecutionBlockId()), 0);
  }

  public static Session createDummySession() {
    return new Session(UUID.randomUUID().toString(), dummyUserInfo.getUserName(), TajoConstants.DEFAULT_DATABASE_NAME);
  }

  public static QueryContext createDummyContext(TajoConf conf) {
    QueryContext context = new QueryContext(conf, createDummySession());
    context.putAll(CommonTestingUtil.getSessionVarsForTest().getAllKeyValus());
    return context;
  }

  /**
   * for test
   * @return The generated QueryId
   */
  public synchronized static QueryId newQueryId() {
    return QueryIdFactory.newQueryId(TajoIdUtils.MASTER_ID_FORMAT.format(0));
  }

  public void setup(String[] names,
                    String[] tablepaths,
                    Schema[] schemas,
                    KeyValueSet option) throws Exception {
    LOG.info("===================================================");
    LOG.info("Starting Test Cluster.");
    LOG.info("===================================================");

    util = new TajoTestingCluster();
    util.startMiniCluster(1);
    conf = util.getConfiguration();
    client = new TajoClientImpl(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = TajoConf.getWarehouseDir(conf);
    fs.mkdirs(rootDir);
    for (int i = 0; i < tablepaths.length; i++) {
      Path localPath = new Path(tablepaths[i]);
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dfsPath = new Path(tablePath, localPath.getName());
      fs.copyFromLocalFile(localPath, dfsPath);
      TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV, option);

      // Add fake table statistic data to tables.
      // It gives more various situations to unit tests.
      TableStats stats = new TableStats();
      stats.setNumBytes(TPCH.tableVolumes.get(names[i]));
      TableDesc tableDesc = new TableDesc(
          CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, names[i]), schemas[i], meta,
          tablePath.toUri());
      tableDesc.setStats(stats);
      util.getMaster().getCatalog().createTable(tableDesc);
    }

    LOG.info("===================================================");
    LOG.info("Test Cluster ready and test table created.");
    LOG.info("===================================================");

  }

  public TajoTestingCluster getTestingCluster() {
    return util;
  }

  public ResultSet execute(String query) throws IOException, ServiceException {
    return client.executeQueryAndGetResult(query);
  }

  public void shutdown() throws IOException {
    if(client != null) {
      client.close();
    }
    if(util != null) {
      util.shutdownMiniCluster();
    }
  }

  public static Path getResourcePath(String path, String suffix) {
    URL resultBaseURL = ClassLoader.getSystemResource(path);
    return new Path(resultBaseURL.toString(), suffix);
  }

  public static Path getResultPath(Class clazz, String fileName) {
    return new Path (getResourcePath("results", clazz.getSimpleName()), fileName);
  }

  public static String getResultText(Class clazz, String fileName) throws IOException {
    FileSystem localFS = FileSystem.getLocal(new Configuration());
    Path path = getResultPath(clazz, fileName);
    Preconditions.checkState(localFS.exists(path) && localFS.isFile(path));
    return FileUtil.readTextFile(new File(path.toUri()));
  }
}
