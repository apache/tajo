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

package org.apache.tajo.querymaster;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.event.QueryEvent;
import org.apache.tajo.master.event.QueryEventType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

public class TestDirectOutputCommitter {

  private static TajoTestingCluster cluster;
  private static TajoClient client;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    client = cluster.newTajoClient();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.unsetSessionVariables(Arrays.asList(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));
    client.close();
  }

  private static void setSessionVariable(boolean value) {
    Map<String, String> variables = new HashMap<>();
    variables.put(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname(), Boolean.toString(value));
    client.updateSessionVariables(variables);
  }


  @Test(timeout = 10000)
  public final void testInsertWithMixedSessionVariable() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    ClientProtos.QueryInfoProto queryInfo = null;
    CatalogService catalog = cluster.getMaster().getCatalog();
    FileSystem fs = cluster.getDefaultFileSystem();

    setSessionVariable(false);
    assertEquals("false", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    // Create partition table
    String tableName = CatalogUtil.normalizeIdentifier(name.getMethodName());

    res = client.executeQueryAndGetResult("create table " + tableName + " ( L_ORDERKEY bigint, L_PARTKEY bigint, " +
      " L_SUPPKEY bigint, L_LINENUMBER bigint, L_QUANTITY double, L_EXTENDEDPRICE double, L_DISCOUNT double, " +
      " L_TAX double, L_RETURNFLAG text, L_LINESTATUS text, L_SHIPDATE text, L_COMMITDATE text, L_RECEIPTDATE text, " +
      " L_SHIPINSTRUCT text, L_SHIPMODE text, L_COMMENT text)");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertNotNull(desc);

    // Insert overwrite the data without direct output committer
    response = client.executeQuery("insert overwrite into " + tableName + " select * from lineitem");
    QueryId queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }
    assertNotNull(queryInfo);

    // Insert overwrite the data with direct output committer
    setSessionVariable(true);
    assertEquals("true", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    response = client.executeQuery("insert into " + tableName + " select * from lineitem");
    queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }
    assertNotNull(queryInfo);

    String prefix = "UUID-" + queryId.toString().substring(2).replaceAll("_", "-");

    FileStatus[] files = fs.listStatus(new Path(desc.getUri()));
    assertEquals(2, files.length);

    // Make sure whether previous data file and new data file exist or not.
    int existingFileCount = 0;
    int addedFileCount = 0;
    for(FileStatus file : files) {
      if (file.getPath().getName().toString().startsWith(prefix)) {
        addedFileCount++;
      } else {
        existingFileCount++;
      }
    }
    assertEquals(1, existingFileCount);
    assertEquals(1, addedFileCount);

    // Check the status of query history from catalog
    List<CatalogProtos.DirectOutputCommitHistoryProto> protos  = catalog.getAllDirectOutputCommitHistories();
    boolean historyFound = false;
    for (CatalogProtos.DirectOutputCommitHistoryProto proto : protos) {
      if (proto.getQueryId().equals(queryId.toString())
        && (proto.getQueryState().equals(TajoProtos.QueryState.QUERY_SUCCEEDED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_FAILED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_KILLED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_ERROR.name())
      )) {
        historyFound = true;
      }
    }
    assertTrue(historyFound);

    res = client.executeQueryAndGetResult("DROP TABLE " + tableName + " PURGE");
    res.close();
  }

  @Test(timeout = 10000)
  public final void testRecoverAfterKillQueryWithNonPartitionedTable() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    ClientProtos.QueryInfoProto queryInfo = null;
    QueryMasterTask queryMasterTask = null;
    CatalogService catalog = cluster.getMaster().getCatalog();
    FileSystem fs = cluster.getDefaultFileSystem();

    setSessionVariable(true);
    assertEquals("true", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    // Create partition table
    String tableName = CatalogUtil.normalizeIdentifier(name.getMethodName());

    res = client.executeQueryAndGetResult("create table " + tableName + " ( L_ORDERKEY bigint, L_PARTKEY bigint, " +
      " L_SUPPKEY bigint, L_LINENUMBER bigint, L_QUANTITY double, L_EXTENDEDPRICE double, L_DISCOUNT double, " +
      " L_TAX double, L_RETURNFLAG text, L_LINESTATUS text, L_SHIPDATE text, L_COMMITDATE text, L_RECEIPTDATE text, " +
      " L_SHIPINSTRUCT text, L_SHIPMODE text, L_COMMENT text)");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertNotNull(desc);

    // Insert overwrite the data
    response = client.executeQuery("insert overwrite into " + tableName + " select * from lineitem");
    QueryId queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }
    assertNotNull(queryInfo);

    String prefix = "UUID-" + queryId.toString().substring(2).replaceAll("_", "-");

    FileStatus[] files = fs.listStatus(new Path(desc.getUri()));
    assertEquals(1, files.length);
    for(FileStatus file : files) {
      assertTrue(file.getPath().getName().toString().startsWith(prefix));
    }

    // Kill query when executing inserting data.
    response = client.executeQuery("insert overwrite into " + tableName + " select * from lineitem");
    queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }

      queryMasterTask = cluster.getQueryMasterTask(queryId);
      if (queryMasterTask != null) {
        files = fs.listStatus(new Path(desc.getUri()));
        if (files.length > 1) {
          queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));
          break;
        }
      }
    }
    waitUntilQueryFinish(queryMasterTask.getQuery(), 50, 200, queryMasterTask);

    // Make sure whether previous partitions exists or not.
    files = fs.listStatus(new Path(desc.getUri()));
    assertEquals(1, files.length);
    for(FileStatus file : files) {
      assertTrue(file.getPath().getName().toString().startsWith(prefix));
    }

    // Check the status of query history from catalog
    List<CatalogProtos.DirectOutputCommitHistoryProto> protos  = catalog.getAllDirectOutputCommitHistories();
    boolean historyFound = false;
    for (CatalogProtos.DirectOutputCommitHistoryProto proto : protos) {
      if (proto.getQueryId().equals(queryId.toString())
        && (proto.getQueryState().equals(TajoProtos.QueryState.QUERY_SUCCEEDED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_FAILED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_KILLED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_ERROR.name())
      )) {
        historyFound = true;
      }
    }
    assertTrue(historyFound);

    res = client.executeQueryAndGetResult("DROP TABLE " + tableName + " PURGE");
    res.close();
  }

  @Test(timeout = 10000)
  public final void testRecoverAfterKillQueryWithPartitionedTableByOneColumn() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    ClientProtos.QueryInfoProto queryInfo = null;
    QueryMasterTask queryMasterTask = null;
    CatalogService catalog = cluster.getMaster().getCatalog();
    FileSystem fs = cluster.getDefaultFileSystem();

    setSessionVariable(true);
    assertEquals("true", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    // Create partition table
    String tableName = CatalogUtil.normalizeIdentifier(name.getMethodName());

    res = client.executeQueryAndGetResult("create table " + tableName
      + " (col1 int4, col2 int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    // Insert overwrite the data
    response = client.executeQuery("insert overwrite into " + tableName
        + " (col1, col2, key) select l_orderkey, l_partkey, l_quantity from lineitem");
    QueryId queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }
    assertNotNull(queryInfo);

    // Check the number of partitions and the prefix of each output file
    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitionsOfTable(DEFAULT_DATABASE_NAME, tableName);
    assertEquals(5, partitions.size());

    String prefix = "UUID-" + queryId.toString().substring(2).replaceAll("_", "-");
    for (CatalogProtos.PartitionDescProto partition : partitions) {
      Path path = new Path(partition.getPath());
      FileStatus[] statuses = fs.listStatus(path);
      assertNotNull(statuses);
      assertEquals(1, statuses.length);
      assertTrue(statuses[0].getPath().getName().toString().startsWith(prefix));
    }

    // Kill query when executing inserting data.
    response = client.executeQuery("insert into " + tableName
      + " select l_orderkey, l_partkey, l_quantity from lineitem");
    queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }

      queryMasterTask = cluster.getQueryMasterTask(queryId);
      if (queryMasterTask != null) {
        Query query = queryMasterTask.getQuery();
        if (query != null && query.getPartitions() != null) {
          if (query.getPartitions().size() > 2) {
            queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));
            Thread.sleep(100);
            break;
          }
        }
      }
    }
    waitUntilQueryFinish(queryMasterTask.getQuery(), 50, 200, queryMasterTask);

    // Make sure whether previous partitions exists or not.
    for (CatalogProtos.PartitionDescProto partition : partitions) {
      Path path = new Path(partition.getPath());
      FileStatus[] statuses = fs.listStatus(path);
      assertNotNull(statuses);
      assertEquals(1, statuses.length);
      assertTrue(statuses[0].getPath().getName().toString().startsWith(prefix));
    }

    // Check the status of query history from catalog
    List<CatalogProtos.DirectOutputCommitHistoryProto> protos  = catalog.getAllDirectOutputCommitHistories();
    boolean historyFound = false;
    for (CatalogProtos.DirectOutputCommitHistoryProto proto : protos) {
      if (proto.getQueryId().equals(queryId.toString())
        && (proto.getQueryState().equals(TajoProtos.QueryState.QUERY_SUCCEEDED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_FAILED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_KILLED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_ERROR.name())
      )) {
        historyFound = true;
      }
    }
    assertTrue(historyFound);

    res = client.executeQueryAndGetResult("DROP TABLE " + tableName + " PURGE");
    res.close();
  }

  @Test(timeout = 10000)
  public final void testRecoverAfterKillQueryWithPartitionedTableByThreeColumns() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    ClientProtos.QueryInfoProto queryInfo = null;
    QueryMasterTask queryMasterTask = null;
    CatalogService catalog = cluster.getMaster().getCatalog();
    FileSystem fs = cluster.getDefaultFileSystem();

    setSessionVariable(true);
    assertEquals("true", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    // Create partition table
    String tableName = CatalogUtil.normalizeIdentifier(name.getMethodName());

    res = client.executeQueryAndGetResult("create table " + tableName
      + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    // Insert overwrite the data
    response = client.executeQuery("insert overwrite into " + tableName
      + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    QueryId queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }
      Thread.sleep(100);
    }
    assertNotNull(queryInfo);

    // Check the number of partitions and the prefix of each output file
    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitionsOfTable(DEFAULT_DATABASE_NAME, tableName);
    assertEquals(5, partitions.size());

    String prefix = "UUID-" + queryId.toString().substring(2).replaceAll("_", "-");
    for (CatalogProtos.PartitionDescProto partition : partitions) {
      Path path = new Path(partition.getPath());
      FileStatus[] statuses = fs.listStatus(path);
      assertNotNull(statuses);
      assertEquals(1, statuses.length);
      assertTrue(statuses[0].getPath().getName().toString().startsWith(prefix));
    }

    // Kill query when executing inserting data.
    response = client.executeQuery("insert into " + tableName
      + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }

      queryMasterTask = cluster.getQueryMasterTask(queryId);
      if (queryMasterTask != null) {
        Query query = queryMasterTask.getQuery();
        if (query != null && query.getPartitions() != null) {
          if (query.getPartitions().size() > 2) {
            queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));
            Thread.sleep(100);
            break;
          }
        }
      }
    }
    waitUntilQueryFinish(queryMasterTask.getQuery(), 50, 200, queryMasterTask);

    // Make sure whether previous partitions exists or not.
    for (CatalogProtos.PartitionDescProto partition : partitions) {
      Path path = new Path(partition.getPath());
      FileStatus[] statuses = fs.listStatus(path);
      assertNotNull(statuses);
      assertEquals(1, statuses.length);
      assertTrue(statuses[0].getPath().getName().toString().startsWith(prefix));
    }

    // Check the status of query history from catalog
    List<CatalogProtos.DirectOutputCommitHistoryProto> protos  = catalog.getAllDirectOutputCommitHistories();
    boolean historyFound = false;
    for (CatalogProtos.DirectOutputCommitHistoryProto proto : protos) {
      if (proto.getQueryId().equals(queryId.toString())) {
        historyFound = true;
      }
    }
    assertTrue(historyFound);

    res = client.executeQueryAndGetResult("DROP TABLE " + tableName + " PURGE");
    res.close();
  }

  @Test(timeout = 10000)
  public final void testRemoveAllFilesAfterKillQueryWithPartitionedTableByOneColumn() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    ClientProtos.QueryInfoProto queryInfo = null;
    QueryMasterTask queryMasterTask = null;
    CatalogService catalog = cluster.getMaster().getCatalog();
    FileSystem fs = cluster.getDefaultFileSystem();

    setSessionVariable(true);
    assertEquals("true", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    // Create partition table
    String tableName = CatalogUtil.normalizeIdentifier(name.getMethodName());

    res = client.executeQueryAndGetResult("create table " + tableName
      + " (col1 int4, col2 int4) partition by column(key float8) ");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertNotNull(desc);

    // Kill query when executing inserting data.
    response = client.executeQuery("insert into " + tableName
      + " select l_orderkey, l_partkey, l_quantity from lineitem");
    QueryId queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }

      queryMasterTask = cluster.getQueryMasterTask(queryId);
      if (queryMasterTask != null) {
        Query query = queryMasterTask.getQuery();
        if (query != null && query.getPartitions() != null) {
          if (query.getPartitions().size() > 2) {
            queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));
            Thread.sleep(100);
            break;
          }
        }
      }
    }
    waitUntilQueryFinish(queryMasterTask.getQuery(), 50, 200, queryMasterTask);

    // Check the number and file count of partitions
    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitionsOfTable(DEFAULT_DATABASE_NAME, tableName);
    assertEquals(0, partitions.size());
    ContentSummary summary = fs.getContentSummary(new Path(desc.getUri()));
    assertEquals(0, summary.getFileCount());

    // Check the status of query history from catalog
    List<CatalogProtos.DirectOutputCommitHistoryProto> protos  = catalog.getAllDirectOutputCommitHistories();
    boolean historyFound = false;
    for (CatalogProtos.DirectOutputCommitHistoryProto proto : protos) {
      if (proto.getQueryId().equals(queryId.toString())
        && (proto.getQueryState().equals(TajoProtos.QueryState.QUERY_SUCCEEDED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_FAILED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_KILLED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_ERROR.name())
      )) {
        historyFound = true;
      }

    }
    assertTrue(historyFound);

    res = client.executeQueryAndGetResult("DROP TABLE " + tableName + " PURGE");
    res.close();
  }


  @Test(timeout = 10000)
  public final void testRemoveAllFilesAfterKillQueryWithPartitionedTableByThreeColumns() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    ClientProtos.QueryInfoProto queryInfo = null;
    QueryMasterTask queryMasterTask = null;
    CatalogService catalog = cluster.getMaster().getCatalog();
    FileSystem fs = cluster.getDefaultFileSystem();

    setSessionVariable(true);
    assertEquals("true", client.getSessionVariable(SessionVars.DIRECT_OUTPUT_COMMITTER_ENABLED.keyname()));

    // Create partition table
    String tableName = CatalogUtil.normalizeIdentifier(name.getMethodName());

    res = client.executeQueryAndGetResult("create table " + tableName
      + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertNotNull(desc);

    // Kill query when executing inserting data.
    response = client.executeQuery("insert into " + tableName
      + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    QueryId queryId = new QueryId(response.getQueryId());

    while (true) {
      queryInfo = client.getQueryInfo(queryId);
      if (queryInfo != null && queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }

      queryMasterTask = cluster.getQueryMasterTask(queryId);
      if (queryMasterTask != null) {
        Query query = queryMasterTask.getQuery();
        if (query != null && query.getPartitions() != null) {
          if (query.getPartitions().size() > 2) {
            queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));
            Thread.sleep(100);
            break;
          }
        }
      }
    }
    waitUntilQueryFinish(queryMasterTask.getQuery(), 50, 200, queryMasterTask);

    // Check the number and file count of partitions
    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitionsOfTable(DEFAULT_DATABASE_NAME, tableName);
    assertEquals(0, partitions.size());
    ContentSummary summary = fs.getContentSummary(new Path(desc.getUri()));
    assertEquals(0, summary.getFileCount());

    // Check the status of query history from catalog
    List<CatalogProtos.DirectOutputCommitHistoryProto> protos  = catalog.getAllDirectOutputCommitHistories();
    boolean historyFound = false;
    for (CatalogProtos.DirectOutputCommitHistoryProto proto : protos) {
      if (proto.getQueryId().equals(queryId.toString())
      && (proto.getQueryState().equals(TajoProtos.QueryState.QUERY_SUCCEEDED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_FAILED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_KILLED.name())
        || proto.getQueryState().equals(TajoProtos.QueryState.QUERY_ERROR.name())
      )) {
        historyFound = true;
      }
    }
    assertTrue(historyFound);

    res = client.executeQueryAndGetResult("DROP TABLE " + tableName + " PURGE");
    res.close();
  }

  private void waitUntilQueryFinish(Query query, int delay, int maxRetry, QueryMasterTask queryMasterTask)
    throws Exception {
    int i = 0;

    try {
      while (query == null || (query.getSynchronizedState() != TajoProtos.QueryState.QUERY_KILLED
        && query.getSynchronizedState() != TajoProtos.QueryState.QUERY_ERROR)) {
        Thread.sleep(delay);
        if (++i > maxRetry) {
          throw new IOException("Exceeded the maximum retry (" + maxRetry + ") actual state: "
            + query != null ? String.valueOf(query.getSynchronizedState()) : String.valueOf(query));
        }
      }
    } finally {
      queryMasterTask.stop();
    }
  }

}
