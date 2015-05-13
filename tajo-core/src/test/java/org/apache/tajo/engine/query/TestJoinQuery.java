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

package org.apache.tajo.engine.query;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestJoinQuery extends QueryTestCaseBase {

  public TestJoinQuery(String joinOption) throws Exception {
    super(TajoConstants.DEFAULT_DATABASE_NAME, joinOption);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (joinOption.indexOf("NoBroadcast") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "-1");
    }

    if (joinOption.indexOf("Hash") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
    }
    if (joinOption.indexOf("Sort") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
    }

    executeDDL("create_lineitem_large_ddl.sql", "lineitem_large");
    executeDDL("create_customer_large_ddl.sql", "customer_large");
    executeDDL("create_orders_large_ddl.sql", "orders_large");
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"Hash_NoBroadcast"},
        {"Sort_NoBroadcast"},
        {"Hash"},
        {"Sort"},
    });
  }

  @AfterClass
  public static void classTearDown() {
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);
  }

  protected void createOuterJoinTestTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{ "1|table12-1" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"2|table13-2", "3|table13-3" };
    TajoTestingCluster.createTable("table13", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4" };
    TajoTestingCluster.createTable("table14", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{};
    TajoTestingCluster.createTable("table15", schema, tableOptions, data);
  }

  protected void dropOuterJoinTestTable() throws Exception {
    executeString("DROP TABLE table11 PURGE;");
    executeString("DROP TABLE table12 PURGE;");
    executeString("DROP TABLE table13 PURGE;");
    executeString("DROP TABLE table14 PURGE;");
    executeString("DROP TABLE table15 PURGE;");
  }

  interface TupleCreator {
    Tuple createTuple(String[] columnDatas);
  }

  protected void createMultiFile(String tableName, int numRowsEachFile, TupleCreator tupleCreator) throws Exception {
    // make multiple small file
    String multiTableName = tableName + "_multifile";
    executeDDL(multiTableName + "_ddl.sql", null);

    TableDesc table = client.getTableDesc(multiTableName);
    assertNotNull(table);

    TableMeta tableMeta = table.getMeta();
    Schema schema = table.getLogicalSchema();

    File file = new File("src/test/tpch/" + tableName + ".tbl");

    if (!file.exists()) {
      file = new File(System.getProperty("user.dir") + "/tajo-core/src/test/tpch/" + tableName + ".tbl");
    }
    String[] rows = FileUtil.readTextFile(file).split("\n");

    assertTrue(rows.length > 0);

    int fileIndex = 0;

    Appender appender = null;
    for (int i = 0; i < rows.length; i++) {
      if (i % numRowsEachFile == 0) {
        if (appender != null) {
          appender.flush();
          appender.close();
        }
        Path dataPath = new Path(table.getPath().toString(), fileIndex + ".csv");
        fileIndex++;
        appender = ((FileStorageManager) StorageManager.getFileStorageManager(conf))
            .getAppender(tableMeta, schema, dataPath);
        appender.init();
      }
      String[] columnDatas = rows[i].split("\\|");
      Tuple tuple = tupleCreator.createTuple(columnDatas);
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
  }

  protected void addEmptyDataFile(String tableName, boolean isPartitioned) throws Exception {
    TableDesc table = client.getTableDesc(tableName);

    Path path = new Path(table.getPath());
    FileSystem fs = path.getFileSystem(conf);
    if (isPartitioned) {
      List<Path> partitionPathList = getPartitionPathList(fs, path);
      for (Path eachPath: partitionPathList) {
        Path dataPath = new Path(eachPath, 0 + "_empty.csv");
        OutputStream out = fs.create(dataPath);
        out.close();
      }
    } else {
      Path dataPath = new Path(path, 0 + "_empty.csv");
      OutputStream out = fs.create(dataPath);
      out.close();
    }
  }

  protected List<Path> getPartitionPathList(FileSystem fs, Path path) throws Exception {
    FileStatus[] files = fs.listStatus(path);
    List<Path> paths = new ArrayList<Path>();
    if (files != null) {
      for (FileStatus eachFile: files) {
        if (eachFile.isFile()) {
          paths.add(path);
          return paths;
        } else {
          paths.addAll(getPartitionPathList(fs, eachFile.getPath()));
        }
      }
    }

    return paths;
  }
}
