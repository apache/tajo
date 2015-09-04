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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.runners.Parameterized.Parameters;

import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestJoinQuery extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestJoinQuery.class);
  private static int reference = 0;

  public TestJoinQuery(String joinOption) throws Exception {
    super(TajoConstants.DEFAULT_DATABASE_NAME, joinOption);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "true");
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD.varname,
        "" + 5);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD.varname,
        "" + 2);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (joinOption.indexOf("NoBroadcast") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
    }

    if (joinOption.indexOf("Hash") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(256));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(256));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256));
    }
    if (joinOption.indexOf("Sort") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(0));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(0));
    }
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

  public static void setup() throws Exception {
    if (reference++ == 0) {
      createCommonTables();
    }
  }

  public static void classTearDown() throws SQLException {
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD.varname,
        ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD.varname,
        ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (--reference == 0) {
      dropCommonTables();
    }
  }

  protected static void createCommonTables() throws Exception {
    LOG.info("Create common tables for join tests");

    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{"1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5"};
    TajoTestingCluster.createTable("jointable11", schema, tableOptions, data, 2);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"1|table12-1", "2|table12-2"};
    TajoTestingCluster.createTable("jointable12", schema, tableOptions, data, 2);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"2|table13-2", "3|table13-3"};
    TajoTestingCluster.createTable("jointable13", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4"};
    TajoTestingCluster.createTable("jointable14", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{};
    TajoTestingCluster.createTable("jointable15", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"1000000|a", "1000001|b", "2|c", "3|d", "4|e"};
    TajoTestingCluster.createTable("jointable1", schema, tableOptions, data, 1);

    data = new String[10000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i + "|" + "this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable" + i;
    }
    TajoTestingCluster.createTable("jointable_large", schema, tableOptions, data, 2);

    // According to node type(leaf or non-leaf) Broadcast join is determined differently by Repartitioner.
    // testMultipleBroadcastDataFileWithZeroLength testcase is for the leaf node
    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });
    addEmptyDataFile("nation_multifile", false);
  }

  protected static void dropCommonTables() throws SQLException {
    LOG.info("Clear common tables for join tests");

    client.executeQuery("DROP TABLE IF EXISTS jointable11 PURGE;");
    client.executeQuery("DROP TABLE IF EXISTS jointable12 PURGE;");
    client.executeQuery("DROP TABLE IF EXISTS jointable13 PURGE;");
    client.executeQuery("DROP TABLE IF EXISTS jointable14 PURGE;");
    client.executeQuery("DROP TABLE IF EXISTS jointable15 PURGE;");
    client.executeQuery("DROP TABLE IF EXISTS jointable1 PURGE");
    client.executeQuery("DROP TABLE IF EXISTS jointable_large PURGE");
    client.executeQuery("DROP TABLE IF EXISTS nation_multifile PURGE");
  }

  interface TupleCreator {
    Tuple createTuple(String[] columnDatas);
  }

  private static String buildSchemaString(String tableName) throws TajoException {
    TableDesc desc = client.getTableDesc(tableName);
    StringBuffer sb = new StringBuffer();
    for (Column column : desc.getSchema().getRootColumns()) {
      sb.append(column.getSimpleName()).append(" ").append(column.getDataType().getType());
      TajoDataTypes.DataType dataType = column.getDataType();
      if (dataType.getLength() > 0) {
        sb.append("(").append(dataType.getLength()).append(")");
      }
      sb.append(",");
    }
    sb.deleteCharAt(sb.length()-1);
    return sb.toString();
  }

  private static String buildMultifileDDlString(String tableName) throws TajoException {
    String multiTableName = tableName + "_multifile";
    StringBuilder sb = new StringBuilder("create table ").append(multiTableName).append(" (");
    sb.append(buildSchemaString(tableName)).append(" )");
    return sb.toString();
  }

  protected static void createMultiFile(String tableName, int numRowsEachFile, TupleCreator tupleCreator) throws Exception {
    // make multiple small file
    String multiTableName = tableName + "_multifile";
    String sql = buildMultifileDDlString(tableName);
    client.executeQueryAndGetResult(sql);

    TableDesc table = client.getTableDesc(multiTableName);
    assertNotNull(table);

    TableMeta tableMeta = table.getMeta();
    Schema schema = table.getLogicalSchema();

    String[] rows = FileUtil.readTextFileFromResource("tpch/" + tableName + ".tbl").split("\n");

    assertTrue(rows.length > 0);

    int fileIndex = 0;

    Appender appender = null;
    for (int i = 0; i < rows.length; i++) {
      if (i % numRowsEachFile == 0) {
        if (appender != null) {
          appender.flush();
          appender.close();
        }
        Path dataPath = new Path(table.getUri().toString(), fileIndex + ".csv");
        fileIndex++;
        appender = (((FileTablespace) TablespaceManager.getLocalFs()))
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

  protected static void addEmptyDataFile(String tableName, boolean isPartitioned) throws Exception {
    TableDesc table = client.getTableDesc(tableName);

    Path path = new Path(table.getUri());
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

  protected static List<Path> getPartitionPathList(FileSystem fs, Path path) throws Exception {
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
