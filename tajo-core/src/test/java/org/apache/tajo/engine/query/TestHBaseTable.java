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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TableSpaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.hbase.*;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestHBaseTable extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestHBaseTable.class);

  private static String hostName,zkPort;

  @BeforeClass
  public static void beforeClass() {
    try {
      testingCluster.getHBaseUtil().startHBaseCluster();
      hostName = InetAddress.getLocalHost().getHostName();
      zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
      assertNotNull(hostName);
      assertNotNull(zkPort);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void afterClass() {
    try {
      testingCluster.getHBaseUtil().stopHBaseCluster();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testVerifyCreateHBaseTableRequiredMeta() throws Exception {
    try {
      executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text) " +
          "USING hbase").close();

      fail("hbase table must have 'table' meta");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("HBase mapped table") >= 0);
    }

    try {
      executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text) " +
          "USING hbase " +
          "WITH ('table'='hbase_table')").close();

      fail("hbase table must have 'columns' meta");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("'columns' property is required") >= 0);
    }
  }

  @Test
  public void testCreateHBaseTable() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text, col3 text, col4 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col2:a,col3:,col2:b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table1");

    HTableDescriptor hTableDesc = testingCluster.getHBaseUtil().getTableDescriptor("hbase_table");
    assertNotNull(hTableDesc);
    assertEquals("hbase_table", hTableDesc.getNameAsString());

    HColumnDescriptor[] hColumns = hTableDesc.getColumnFamilies();
    // col1 is mapped to rowkey
    assertEquals(2, hColumns.length);
    assertEquals("col2", hColumns[0].getNameAsString());
    assertEquals("col3", hColumns[1].getNameAsString());

    executeString("DROP TABLE hbase_mapped_table1 PURGE").close();

    HBaseAdmin hAdmin =  new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
    try {
      assertFalse(hAdmin.tableExists("hbase_table"));
    } finally {
      hAdmin.close();
    }
  }

  @Test
  public void testCreateNotExistsExternalHBaseTable() throws Exception {
    try {
      executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table1 (col1 text, col2 text, col3 text, col4 text) " +
          "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key,col2:a,col3:,col2:b', " +
          "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
          "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();
      fail("External table should be a existed table.");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("External table should be a existed table.") >= 0);
    }
  }

  @Test
  public void testCreateRowFieldWithNonText() throws Exception {
    try {
      executeString("CREATE TABLE hbase_mapped_table2 (rk1 int4, rk2 text, col3 text, col4 text) " +
          "USING hbase WITH ('table'='hbase_table', 'columns'='0:key#b,1:key,col3:,col2:b', " +
          "'hbase.rowkey.delimiter'='_', " +
          "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
          "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();
      fail("Key field type should be TEXT type");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("Key field type should be TEXT type") >= 0);
    }
  }

  @Test
  public void testCreateExternalHBaseTable() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table_not_purge"));
    hTableDesc.addFamily(new HColumnDescriptor("col1"));
    hTableDesc.addFamily(new HColumnDescriptor("col2"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table_not_purge', 'columns'=':key,col1:a,col2:,col3:b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    executeString("DROP TABLE external_hbase_mapped_table").close();

    HBaseAdmin hAdmin =  new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
    try {
      assertTrue(hAdmin.tableExists("external_hbase_table_not_purge"));
      hAdmin.disableTable("external_hbase_table_not_purge");
      hAdmin.deleteTable("external_hbase_table_not_purge");
    } finally {
      hAdmin.close();
    }
  }

  @Test
  public void testSimpleSelectQuery() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table"));
    hTableDesc.addFamily(new HColumnDescriptor("col1"));
    hTableDesc.addFamily(new HColumnDescriptor("col2"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key,col1:a,col2:,col3:b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseTablespace) TableSpaceManager.getStorageManager(conf, "HBASE"))
        .getConnection(testingCluster.getHBaseUtil().getConf());
    HTableInterface htable = hconn.getTable("external_hbase_table");

    try {
      for (int i = 0; i < 100; i++) {
        Put put = new Put(String.valueOf(i).getBytes());
        put.add("col1".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        put.add("col1".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        put.add("col2".getBytes(), "k1".getBytes(), ("k1-" + i).getBytes());
        put.add("col2".getBytes(), "k2".getBytes(), ("k2-" + i).getBytes());
        put.add("col3".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        htable.put(put);
      }

      ResultSet res = executeString("select * from external_hbase_mapped_table where rk > '20'");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
      htable.close();
    }
  }

  @Test
  public void testBinaryMappedQuery() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table"));
    hTableDesc.addFamily(new HColumnDescriptor("col1"));
    hTableDesc.addFamily(new HColumnDescriptor("col2"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk int8, col1 text, col2 text, col3 int4)\n " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key#b,col1:a,col2:,col3:b#b', \n" +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "', \n" +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseTablespace) TableSpaceManager.getStorageManager(conf, "HBASE"))
        .getConnection(testingCluster.getHBaseUtil().getConf());
    HTableInterface htable = hconn.getTable("external_hbase_table");

    try {
      for (int i = 0; i < 100; i++) {
        Put put = new Put(Bytes.toBytes((long) i));
        put.add("col1".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        put.add("col1".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        put.add("col2".getBytes(), "k1".getBytes(), ("k1-" + i).getBytes());
        put.add("col2".getBytes(), "k2".getBytes(), ("k2-" + i).getBytes());
        put.add("col3".getBytes(), "b".getBytes(), Bytes.toBytes(i));
        htable.put(put);
      }

      ResultSet res = executeString("select * from external_hbase_mapped_table where rk > 20");
      assertResultSet(res);
      res.close();

      //Projection
      res = executeString("select col3, col2, rk from external_hbase_mapped_table where rk > 95");

      String expected = "col3,col2,rk\n" +
          "-------------------------------\n" +
          "96,{\"k1\":\"k1-96\", \"k2\":\"k2-96\"},96\n" +
          "97,{\"k1\":\"k1-97\", \"k2\":\"k2-97\"},97\n" +
          "98,{\"k1\":\"k1-98\", \"k2\":\"k2-98\"},98\n" +
          "99,{\"k1\":\"k1-99\", \"k2\":\"k2-99\"},99\n";

      assertEquals(expected, resultSetToString(res));
      res.close();

    } finally {
      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
      htable.close();
    }
  }

  @Test
  public void testColumnKeyValueSelectQuery() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table"));
    hTableDesc.addFamily(new HColumnDescriptor("col2"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk1 text, col2_key text, col2_value text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key,col2:key:,col2:value:,col3:', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseTablespace) TableSpaceManager.getStorageManager(conf, "HBASE"))
        .getConnection(testingCluster.getHBaseUtil().getConf());
    HTableInterface htable = hconn.getTable("external_hbase_table");

    try {
      for (int i = 0; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("rk-" + i));
        for (int j = 0; j < 5; j++) {
          put.add("col2".getBytes(), ("key-" + j).getBytes(), Bytes.toBytes("value-" + j));
        }
        put.add("col3".getBytes(), "".getBytes(), ("col3-value-" + i).getBytes());
        htable.put(put);
      }

      ResultSet res = executeString("select * from external_hbase_mapped_table where rk1 >= 'rk-0'");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
      htable.close();
    }
  }

  @Test
  public void testRowFieldSelectQuery() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk1 text, rk2 text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'='0:key,1:key,col3:a', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseTablespace) TableSpaceManager.getStorageManager(conf, "HBASE"))
        .getConnection(testingCluster.getHBaseUtil().getConf());
    HTableInterface htable = hconn.getTable("external_hbase_table");

    try {
      for (int i = 0; i < 100; i++) {
        Put put = new Put(("field1-" + i + "_field2-" + i).getBytes());
        put.add("col3".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        htable.put(put);
      }

      ResultSet res = executeString("select * from external_hbase_mapped_table where rk1 > 'field1-20'");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
      htable.close();
    }
  }

  @Test
  public void testIndexPredication() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:,col3:b', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();


    assertTableExists("hbase_mapped_table");
    HBaseAdmin hAdmin = new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
    hAdmin.tableExists("hbase_table");

    HTable htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");
    try {
      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
      assertEquals(5, keys.getFirst().length);

      DecimalFormat df = new DecimalFormat("000");
      for (int i = 0; i < 100; i++) {
        Put put = new Put(String.valueOf(df.format(i)).getBytes());
        put.add("col1".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        put.add("col1".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        put.add("col2".getBytes(), "k1".getBytes(), ("k1-" + i).getBytes());
        put.add("col2".getBytes(), "k2".getBytes(), ("k2-" + i).getBytes());
        put.add("col3".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        htable.put(put);
      }
      assertIndexPredication(false);

      ResultSet res = executeString("select * from hbase_mapped_table where rk >= '020' and rk <= '055'");
      assertResultSet(res);
      res.close();

      res = executeString("select * from hbase_mapped_table where rk = '021'");
      String expected = "rk,col1,col2,col3\n" +
          "-------------------------------\n" +
          "021,a-21,{\"k1\":\"k1-21\", \"k2\":\"k2-21\"},b-21\n";

      assertEquals(expected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
      htable.close();
      hAdmin.close();
    }
  }

  @Test
  public void testCompositeRowIndexPredication() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, rk2 text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'='0:key,1:key,col1:a,col2:,col3:b', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    HBaseAdmin hAdmin = new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
    hAdmin.tableExists("hbase_table");

    HTable htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");
    try {
      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
      assertEquals(5, keys.getFirst().length);

      DecimalFormat df = new DecimalFormat("000");
      for (int i = 0; i < 100; i++) {
        Put put = new Put((df.format(i) + "_" + df.format(i)).getBytes());
        put.add("col1".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        put.add("col1".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        put.add("col2".getBytes(), "k1".getBytes(), ("k1-" + i).getBytes());
        put.add("col2".getBytes(), "k2".getBytes(), ("k2-" + i).getBytes());
        put.add("col3".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        htable.put(put);
      }

      Scan scan = new Scan();
      scan.setStartRow("021".getBytes());
      scan.setStopRow(("021_" + new String(new char[]{Character.MAX_VALUE})).getBytes());
      Filter filter = new InclusiveStopFilter(scan.getStopRow());
      scan.setFilter(filter);

      ResultScanner scanner = htable.getScanner(scan);
      Result result = scanner.next();
      assertNotNull(result);
      assertEquals("021_021", new String(result.getRow()));
      scanner.close();

      assertIndexPredication(true);

      ResultSet res = executeString("select * from hbase_mapped_table where rk = '021'");
      String expected = "rk,rk2,col1,col2,col3\n" +
          "-------------------------------\n" +
          "021,021,a-21,{\"k1\":\"k1-21\", \"k2\":\"k2-21\"},b-21\n";

      assertEquals(expected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
      htable.close();
      hAdmin.close();
    }
  }

  private void assertIndexPredication(boolean isCompositeRowKey) throws Exception {
    String postFix = isCompositeRowKey ? "_" + new String(new char[]{Character.MAX_VALUE}) : "";
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    ScanNode scanNode = new ScanNode(1);

    // where rk = '021'
    EvalNode evalNodeEq = new BinaryEval(EvalType.EQUAL, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("021")));
    scanNode.setQual(evalNodeEq);
    Tablespace tablespace = TableSpaceManager.getStorageManager(conf, "HBASE");
    List<Fragment> fragments = tablespace.getSplits("hbase_mapped_table", tableDesc, scanNode);
    assertEquals(1, fragments.size());
    assertEquals("021", new String(((HBaseFragment)fragments.get(0)).getStartRow()));
    assertEquals("021" + postFix, new String(((HBaseFragment)fragments.get(0)).getStopRow()));

    // where rk >= '020' and rk <= '055'
    EvalNode evalNode1 = new BinaryEval(EvalType.GEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("020")));
    EvalNode evalNode2 = new BinaryEval(EvalType.LEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("055")));
    EvalNode evalNodeA = new BinaryEval(EvalType.AND, evalNode1, evalNode2);
    scanNode.setQual(evalNodeA);

    fragments = tablespace.getSplits("hbase_mapped_table", tableDesc, scanNode);
    assertEquals(2, fragments.size());
    HBaseFragment fragment1 = (HBaseFragment) fragments.get(0);
    assertEquals("020", new String(fragment1.getStartRow()));
    assertEquals("040", new String(fragment1.getStopRow()));

    HBaseFragment fragment2 = (HBaseFragment) fragments.get(1);
    assertEquals("040", new String(fragment2.getStartRow()));
    assertEquals("055" + postFix, new String(fragment2.getStopRow()));

    // where (rk >= '020' and rk <= '055') or rk = '075'
    EvalNode evalNode3 = new BinaryEval(EvalType.EQUAL, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("075")));
    EvalNode evalNodeB = new BinaryEval(EvalType.OR, evalNodeA, evalNode3);
    scanNode.setQual(evalNodeB);
    fragments = tablespace.getSplits("hbase_mapped_table", tableDesc, scanNode);
    assertEquals(3, fragments.size());
    fragment1 = (HBaseFragment) fragments.get(0);
    assertEquals("020", new String(fragment1.getStartRow()));
    assertEquals("040", new String(fragment1.getStopRow()));

    fragment2 = (HBaseFragment) fragments.get(1);
    assertEquals("040", new String(fragment2.getStartRow()));
    assertEquals("055" + postFix, new String(fragment2.getStopRow()));

    HBaseFragment fragment3 = (HBaseFragment) fragments.get(2);
    assertEquals("075", new String(fragment3.getStartRow()));
    assertEquals("075" + postFix, new String(fragment3.getStopRow()));


    // where (rk >= '020' and rk <= '055') or (rk >= '072' and rk <= '078')
    EvalNode evalNode4 = new BinaryEval(EvalType.GEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("072")));
    EvalNode evalNode5 = new BinaryEval(EvalType.LEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("078")));
    EvalNode evalNodeC = new BinaryEval(EvalType.AND, evalNode4, evalNode5);
    EvalNode evalNodeD = new BinaryEval(EvalType.OR, evalNodeA, evalNodeC);
    scanNode.setQual(evalNodeD);
    fragments = tablespace.getSplits("hbase_mapped_table", tableDesc, scanNode);
    assertEquals(3, fragments.size());

    fragment1 = (HBaseFragment) fragments.get(0);
    assertEquals("020", new String(fragment1.getStartRow()));
    assertEquals("040", new String(fragment1.getStopRow()));

    fragment2 = (HBaseFragment) fragments.get(1);
    assertEquals("040", new String(fragment2.getStartRow()));
    assertEquals("055" + postFix, new String(fragment2.getStopRow()));

    fragment3 = (HBaseFragment) fragments.get(2);
    assertEquals("072", new String(fragment3.getStartRow()));
    assertEquals("078" + postFix, new String(fragment3.getStopRow()));

    // where (rk >= '020' and rk <= '055') or (rk >= '057' and rk <= '059')
    evalNode4 = new BinaryEval(EvalType.GEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("057")));
    evalNode5 = new BinaryEval(EvalType.LEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
        new ConstEval(new TextDatum("059")));
    evalNodeC = new BinaryEval(EvalType.AND, evalNode4, evalNode5);
    evalNodeD = new BinaryEval(EvalType.OR, evalNodeA, evalNodeC);
    scanNode.setQual(evalNodeD);
    fragments = tablespace.getSplits("hbase_mapped_table", tableDesc, scanNode);
    assertEquals(2, fragments.size());

    fragment1 = (HBaseFragment) fragments.get(0);
    assertEquals("020", new String(fragment1.getStartRow()));
    assertEquals("040", new String(fragment1.getStopRow()));

    fragment2 = (HBaseFragment) fragments.get(1);
    assertEquals("040", new String(fragment2.getStartRow()));
    assertEquals("059" + postFix, new String(fragment2.getStopRow()));
  }

  @Test
  public void testNonForwardQuery() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text, col3 int) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:,col3:#b', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    HBaseAdmin hAdmin =  new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
    HTable htable = null;
    try {
      hAdmin.tableExists("hbase_table");
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");
      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
      assertEquals(5, keys.getFirst().length);

      DecimalFormat df = new DecimalFormat("000");
      for (int i = 0; i < 100; i++) {
        Put put = new Put(String.valueOf(df.format(i)).getBytes());
        put.add("col1".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        put.add("col1".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        put.add("col2".getBytes(), "k1".getBytes(), ("k1-" + i).getBytes());
        put.add("col2".getBytes(), "k2".getBytes(), ("k2-" + i).getBytes());
        put.add("col3".getBytes(), "".getBytes(), Bytes.toBytes(i));
        htable.put(put);
      }

      ResultSet res = executeString("select * from hbase_mapped_table");
      assertResultSet(res);
      res.close();
    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
      hAdmin.close();
      if (htable == null) {
        htable.close();
      }
    }
  }

  @Test
  public void testJoin() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text, col3 int8) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:,col3:b#b', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    HBaseAdmin hAdmin =  new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
    HTable htable = null;
    try {
      hAdmin.tableExists("hbase_table");
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");
      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
      assertEquals(5, keys.getFirst().length);

      DecimalFormat df = new DecimalFormat("000");
      for (int i = 0; i < 100; i++) {
        Put put = new Put(String.valueOf(df.format(i)).getBytes());
        put.add("col1".getBytes(), "a".getBytes(), ("a-" + i).getBytes());
        put.add("col1".getBytes(), "b".getBytes(), ("b-" + i).getBytes());
        put.add("col2".getBytes(), "k1".getBytes(), ("k1-" + i).getBytes());
        put.add("col2".getBytes(), "k2".getBytes(), ("k2-" + i).getBytes());
        put.add("col3".getBytes(), "b".getBytes(), Bytes.toBytes((long) i));
        htable.put(put);
      }

      ResultSet res = executeString("select a.rk, a.col1, a.col2, a.col3, b.l_orderkey, b.l_linestatus " +
          "from hbase_mapped_table a " +
          "join default.lineitem b on a.col3 = b.l_orderkey");
      assertResultSet(res);
      res.close();
    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
      hAdmin.close();
      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertInto() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text, col3 int4) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:,col3:b#b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    executeString("insert into hbase_mapped_table " +
        "select l_orderkey::text, l_shipdate, l_returnflag, l_suppkey from default.lineitem ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scan.addFamily(Bytes.toBytes("col2"));
      scan.addFamily(Bytes.toBytes("col3"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1"), Bytes.toBytes("col2"), Bytes.toBytes("col3")},
          new byte[][]{null, Bytes.toBytes("a"), null, Bytes.toBytes("b")},
          new boolean[]{false, false, false, true}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoMultiRegion() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.TEXT);
    schema.addColumn("name", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    DecimalFormat df = new DecimalFormat("000");
    for (int i = 99; i >= 0; i--) {
      datas.add(df.format(i) + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("insert into hbase_mapped_table " +
        "select id, name from base_table ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1")},
          new byte[][]{null, Bytes.toBytes("a")},
          new boolean[]{false, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoMultiRegion2() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a', " +
        "'hbase.split.rowkeys'='1,2,3,4,5,6,7,8,9', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.TEXT);
    schema.addColumn("name", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    for (int i = 99; i >= 0; i--) {
      datas.add(i + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("insert into hbase_mapped_table " +
        "select id, name from base_table ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1")},
          new byte[][]{null, Bytes.toBytes("a")},
          new boolean[]{false, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoMultiRegionWithSplitFile() throws Exception {
    String splitFilePath = currentDatasetPath + "/splits.data";

    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a', " +
        "'hbase.split.rowkeys.file'='" + splitFilePath + "', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.TEXT);
    schema.addColumn("name", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    DecimalFormat df = new DecimalFormat("000");
    for (int i = 99; i >= 0; i--) {
      datas.add(df.format(i) + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("insert into hbase_mapped_table " +
        "select id, name from base_table ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1")},
          new byte[][]{null, Bytes.toBytes("a")},
          new boolean[]{false, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoMultiRegionMultiRowFields() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk1 text, rk2 text, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'='0:key,1:key,col1:a', " +
        "'hbase.split.rowkeys'='001,002,003,004,005,006,007,008,009', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id1", Type.TEXT);
    schema.addColumn("id2", Type.TEXT);
    schema.addColumn("name", Type.TEXT);
    DecimalFormat df = new DecimalFormat("000");
    List<String> datas = new ArrayList<String>();
    for (int i = 99; i >= 0; i--) {
      datas.add(df.format(i) + "|" + (i + 100) + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("insert into hbase_mapped_table " +
        "select id1, id2, name from base_table ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, null, Bytes.toBytes("col1")},
          new byte[][]{null, null, Bytes.toBytes("a")},
          new boolean[]{false, false, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoBinaryMultiRegion() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk int4, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key#b,col1:a', " +
        "'hbase.split.rowkeys'='1,2,3,4,5,6,7,8,9', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    for (int i = 99; i >= 0; i--) {
      datas.add(i + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("insert into hbase_mapped_table " +
        "select id, name from base_table ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1")},
          new byte[][]{null, Bytes.toBytes("a")},
          new boolean[]{true, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoColumnKeyValue() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col2_key text, col2_value text, col3 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col2:key:,col2:value:,col3:', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("rk", Type.TEXT);
    schema.addColumn("col2_key", Type.TEXT);
    schema.addColumn("col2_value", Type.TEXT);
    schema.addColumn("col3", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    for (int i = 20; i >= 0; i--) {
      for (int j = 0; j < 3; j++) {
        datas.add(i + "|ck-" + j + "|value-" + j + "|col3-" + i);
      }
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("insert into hbase_mapped_table " +
        "select rk, col2_key, col2_value, col3 from base_table ").close();

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col2"));
      scan.addFamily(Bytes.toBytes("col3"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col2"), Bytes.toBytes("col3")},
          new byte[][]{null, null, null},
          new boolean[]{false, false, false}, tableDesc.getSchema()));

      ResultSet res = executeString("select * from hbase_mapped_table");

      String expected = "rk,col2_key,col2_value,col3\n" +
          "-------------------------------\n" +
          "0,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-0\n" +
          "1,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-1\n" +
          "10,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-10\n" +
          "11,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-11\n" +
          "12,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-12\n" +
          "13,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-13\n" +
          "14,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-14\n" +
          "15,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-15\n" +
          "16,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-16\n" +
          "17,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-17\n" +
          "18,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-18\n" +
          "19,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-19\n" +
          "2,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-2\n" +
          "20,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-20\n" +
          "3,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-3\n" +
          "4,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-4\n" +
          "5,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-5\n" +
          "6,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-6\n" +
          "7,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-7\n" +
          "8,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-8\n" +
          "9,[\"ck-0\", \"ck-1\", \"ck-2\"],[\"value-0\", \"value-1\", \"value-2\"],col3-9\n";

      assertEquals(expected, resultSetToString(res));
      res.close();

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoDifferentType() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a', " +
        "'hbase.split.rowkeys'='1,2,3,4,5,6,7,8,9', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");

    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    for (int i = 99; i >= 0; i--) {
      datas.add(i + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    try {
      executeString("insert into hbase_mapped_table " +
          "select id, name from base_table ").close();
      fail("If inserting data type different with target table data type, should throw exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("is different column type with") >= 0);
    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    }
  }

  @Test
  public void testInsertIntoRowField() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk1 text, rk2 text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'='0:key,1:key,col1:a,col2:,col3:b', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();


    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    executeString("insert into hbase_mapped_table " +
        "select l_orderkey::text, l_partkey::text, l_shipdate, l_returnflag, l_suppkey::text from default.lineitem ");

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scan.addFamily(Bytes.toBytes("col2"));
      scan.addFamily(Bytes.toBytes("col3"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1"), Bytes.toBytes("col2"), Bytes.toBytes("col3")},
          new byte[][]{null, Bytes.toBytes("a"), Bytes.toBytes(""), Bytes.toBytes("b")},
          new boolean[]{false, false, false, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testCATS() throws Exception {
    // create test table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.TEXT);
    schema.addColumn("name", Type.TEXT);
    List<String> datas = new ArrayList<String>();
    DecimalFormat df = new DecimalFormat("000");
    for (int i = 99; i >= 0; i--) {
      datas.add(df.format(i) + "|value" + i);
    }
    TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
        schema, tableOptions, datas.toArray(new String[]{}), 2);

    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')" +
        " as " +
        "select id, name from base_table"
    ).close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scanner = htable.getScanner(scan);

      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1")},
          new byte[][]{null, Bytes.toBytes("a")},
          new boolean[]{false, false}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoUsingPut() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text, col3 int4) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:,col3:b#b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");
    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

    Map<String, String> sessions = new HashMap<String, String>();
    sessions.put(HBaseStorageConstants.INSERT_PUT_MODE, "true");
    client.updateSessionVariables(sessions);

    HTable htable = null;
    ResultScanner scanner = null;
    try {
      executeString("insert into hbase_mapped_table " +
          "select l_orderkey::text, l_shipdate, l_returnflag, l_suppkey from default.lineitem ").close();

      htable = new HTable(testingCluster.getHBaseUtil().getConf(), "hbase_table");

      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("col1"));
      scan.addFamily(Bytes.toBytes("col2"));
      scan.addFamily(Bytes.toBytes("col3"));
      scanner = htable.getScanner(scan);

      // result is dirrerent with testInsertInto because l_orderkey is not unique.
      assertStrings(resultSetToString(scanner,
          new byte[][]{null, Bytes.toBytes("col1"), Bytes.toBytes("col2"), Bytes.toBytes("col3")},
          new byte[][]{null, Bytes.toBytes("a"), null, Bytes.toBytes("b")},
          new boolean[]{false, false, false, true}, tableDesc.getSchema()));

    } finally {
      executeString("DROP TABLE hbase_mapped_table PURGE").close();

      client.unsetSessionVariables(TUtil.newList(HBaseStorageConstants.INSERT_PUT_MODE));

      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertIntoLocation() throws Exception {
    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table");

    try {
      // create test table
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.set(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      tableOptions.set(StorageConstants.CSVFILE_NULL, "\\\\N");

      Schema schema = new Schema();
      schema.addColumn("id", Type.TEXT);
      schema.addColumn("name", Type.TEXT);
      schema.addColumn("comment", Type.TEXT);
      List<String> datas = new ArrayList<String>();
      DecimalFormat df = new DecimalFormat("000");
      for (int i = 99; i >= 0; i--) {
        datas.add(df.format(i) + "|value" + i + "|comment-" + i);
      }
      TajoTestingCluster.createTable(getCurrentDatabase() + ".base_table",
          schema, tableOptions, datas.toArray(new String[]{}), 2);

      executeString("insert into location '/tmp/hfile_test' " +
          "select id, name, comment from base_table ").close();

      FileSystem fs = testingCluster.getDefaultFileSystem();
      Path path = new Path("/tmp/hfile_test");
      assertTrue(fs.exists(path));

      FileStatus[] files = fs.listStatus(path);
      assertNotNull(files);
      assertEquals(2, files.length);

      int index = 0;
      for (FileStatus eachFile: files) {
        assertEquals("/tmp/hfile_test/part-01-00000" + index + "-00" + index, eachFile.getPath().toUri().getPath());
        for (FileStatus subFile: fs.listStatus(eachFile.getPath())) {
          assertTrue(subFile.isFile());
          assertTrue(subFile.getLen() > 0);
        }
        index++;
      }
    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    }
  }

  private String resultSetToString(ResultScanner scanner,
                                   byte[][] cfNames, byte[][] qualifiers,
                                   boolean[] binaries,
                                   Schema schema) throws Exception {
    StringBuilder sb = new StringBuilder();
    Result result = null;
    while ( (result = scanner.next()) != null ) {
      if (binaries[0]) {
        sb.append(HBaseBinarySerializerDeserializer.deserialize(schema.getColumn(0), result.getRow()).asChar());
      } else {
        sb.append(new String(result.getRow()));
      }

      for (int i = 0; i < cfNames.length; i++) {
        if (cfNames[i] == null) {
          //rowkey
          continue;
        }
        if (qualifiers[i] == null) {
          Map<byte[], byte[]> values = result.getFamilyMap(cfNames[i]);
          if (values == null) {
            sb.append(", null");
          } else {
            sb.append(", {");
            String delim = "";
            for (Map.Entry<byte[], byte[]> valueEntry: values.entrySet()) {
              byte[] keyBytes = valueEntry.getKey();
              byte[] valueBytes = valueEntry.getValue();

              if (binaries[i]) {
                sb.append(delim).append("\"").append(keyBytes == null ? "" : Bytes.toLong(keyBytes)).append("\"");
                sb.append(": \"").append(HBaseBinarySerializerDeserializer.deserialize(schema.getColumn(i), valueBytes)).append("\"");
              } else {
                sb.append(delim).append("\"").append(keyBytes == null ? "" : new String(keyBytes)).append("\"");
                sb.append(": \"").append(HBaseTextSerializerDeserializer.deserialize(schema.getColumn(i), valueBytes)).append("\"");
              }
              delim = ", ";
            }
            sb.append("}");
          }
        } else {
          byte[] value = result.getValue(cfNames[i], qualifiers[i]);
          if (value == null) {
            sb.append(", null");
          } else {
            if (binaries[i]) {
              sb.append(", ").append(HBaseBinarySerializerDeserializer.deserialize(schema.getColumn(i), value));
            } else {
              sb.append(", ").append(HBaseTextSerializerDeserializer.deserialize(schema.getColumn(i), value));
            }
          }
        }
      }
      sb.append("\n");
    }

    return sb.toString();
  }
}
