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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.hbase.HBaseBinarySerializerDeserializer;
import org.apache.tajo.storage.hbase.HBaseFragment;
import org.apache.tajo.storage.hbase.HBaseStorageManager;
import org.apache.tajo.storage.hbase.HBaseTextSerializerDeserializer;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestHBaseTable extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestHBaseTable.class);

  @BeforeClass
  public static void beforeClass() {
    try {
      testingCluster.getHBaseUtil().startHBaseCluster();
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

    try {
      executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text) " +
          "USING hbase " +
          "WITH ('table'='hbase_table', 'columns'='col1:,col2:')").close();

      fail("hbase table must have 'hbase.zookeeper.quorum' meta");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("HBase mapped table") >= 0);
    }
  }

  @Test
  public void testCreateHBaseTable() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key,col1:a,col2:,col3:b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseStorageManager)StorageManager.getStorageManager(conf, StoreType.HBASE))
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
      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
    } finally {
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

    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk int8, col1 text, col2 text, col3 int4)\n " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key#b,col1:a,col2:,col3:b#b', \n" +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "', \n" +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseStorageManager)StorageManager.getStorageManager(conf, StoreType.HBASE))
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
      res = executeString("select col3 from external_hbase_mapped_table where rk > 95");

      String expected = "col3\n" +
          "-------------------------------\n" +
          "96\n" +
          "97\n" +
          "98\n" +
          "99\n";

      assertEquals(expected, resultSetToString(res));
      res.close();

      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
    } finally {
      htable.close();
    }
  }

  @Test
  public void testRowFieldSelectQuery() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk1 text, rk2 text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'='0:key,1:key,col3:a', " +
        "'hbase.rowkey.delimiter'='_', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    HConnection hconn = ((HBaseStorageManager)StorageManager.getStorageManager(conf, StoreType.HBASE))
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
      executeString("DROP TABLE external_hbase_mapped_table PURGE").close();
    } finally {
      htable.close();
    }
  }

  @Test
  public void testIndexPredication() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    executeString("CREATE TABLE hbase_mapped_table (rk text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col1:a,col2:,col3:b', " +
        "'hbase.split.rowkeys'='010,040,060,080', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();


    assertTableExists("hbase_mapped_table");
    HBaseAdmin hAdmin =  new HBaseAdmin(testingCluster.getHBaseUtil().getConf());
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

      TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), "hbase_mapped_table");

      // where rk >= '020' and rk <= '055'
      ScanNode scanNode = new ScanNode(1);
      EvalNode evalNode1 = new BinaryEval(EvalType.GEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("020")));
      EvalNode evalNode2 = new BinaryEval(EvalType.LEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("055")));
      EvalNode evalNodeA = new BinaryEval(EvalType.AND, evalNode1, evalNode2);

      scanNode.setQual(evalNodeA);

      StorageManager storageManager = StorageManager.getStorageManager(conf, StoreType.HBASE);
      List<Fragment> fragments = storageManager.getSplits("hbase_mapped_table", tableDesc, scanNode);

      assertEquals(2, fragments.size());
      HBaseFragment fragment1 = (HBaseFragment) fragments.get(0);
      assertEquals("020", new String(fragment1.getStartRow()));
      assertEquals("040", new String(fragment1.getStopRow()));

      HBaseFragment fragment2 = (HBaseFragment) fragments.get(1);
      assertEquals("040", new String(fragment2.getStartRow()));
      assertEquals("055", new String(fragment2.getStopRow()));


      // where (rk >= '020' and rk <= '055') or rk = '075'
      EvalNode evalNode3 = new BinaryEval(EvalType.EQUAL, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("075")));
      EvalNode evalNodeB = new BinaryEval(EvalType.OR, evalNodeA, evalNode3);
      scanNode.setQual(evalNodeB);
      fragments = storageManager.getSplits("hbase_mapped_table", tableDesc, scanNode);
      assertEquals(3, fragments.size());
      fragment1 = (HBaseFragment) fragments.get(0);
      assertEquals("020", new String(fragment1.getStartRow()));
      assertEquals("040", new String(fragment1.getStopRow()));

      fragment2 = (HBaseFragment) fragments.get(1);
      assertEquals("040", new String(fragment2.getStartRow()));
      assertEquals("055", new String(fragment2.getStopRow()));

      HBaseFragment fragment3 = (HBaseFragment) fragments.get(2);
      assertEquals("075", new String(fragment3.getStartRow()));
      assertEquals("075", new String(fragment3.getStopRow()));


      // where (rk >= '020' and rk <= '055') or (rk >= '072' and rk <= '078')
      EvalNode evalNode4 = new BinaryEval(EvalType.GEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("072")));
      EvalNode evalNode5 = new BinaryEval(EvalType.LEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("078")));
      EvalNode evalNodeC = new BinaryEval(EvalType.AND, evalNode4, evalNode5);
      EvalNode evalNodeD = new BinaryEval(EvalType.OR, evalNodeA, evalNodeC);
      scanNode.setQual(evalNodeD);
      fragments = storageManager.getSplits("hbase_mapped_table", tableDesc, scanNode);
      assertEquals(3, fragments.size());

      fragment1 = (HBaseFragment) fragments.get(0);
      assertEquals("020", new String(fragment1.getStartRow()));
      assertEquals("040", new String(fragment1.getStopRow()));

      fragment2 = (HBaseFragment) fragments.get(1);
      assertEquals("040", new String(fragment2.getStartRow()));
      assertEquals("055", new String(fragment2.getStopRow()));

      fragment3 = (HBaseFragment) fragments.get(2);
      assertEquals("072", new String(fragment3.getStartRow()));
      assertEquals("078", new String(fragment3.getStopRow()));

      // where (rk >= '020' and rk <= '055') or (rk >= '057' and rk <= '059')
      evalNode4 = new BinaryEval(EvalType.GEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("057")));
      evalNode5 = new BinaryEval(EvalType.LEQ, new FieldEval(tableDesc.getLogicalSchema().getColumn("rk")),
          new ConstEval(new TextDatum("059")));
      evalNodeC = new BinaryEval(EvalType.AND, evalNode4, evalNode5);
      evalNodeD = new BinaryEval(EvalType.OR, evalNodeA, evalNodeC);
      scanNode.setQual(evalNodeD);
      fragments = storageManager.getSplits("hbase_mapped_table", tableDesc, scanNode);
      assertEquals(2, fragments.size());

      fragment1 = (HBaseFragment) fragments.get(0);
      assertEquals("020", new String(fragment1.getStartRow()));
      assertEquals("040", new String(fragment1.getStopRow()));

      fragment2 = (HBaseFragment) fragments.get(1);
      assertEquals("040", new String(fragment2.getStartRow()));
      assertEquals("059", new String(fragment2.getStopRow()));

      ResultSet res = executeString("select * from hbase_mapped_table where rk >= '020' and rk <= '055'");
      assertResultSet(res);
      res.close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
      htable.close();
      hAdmin.close();
    }
  }

  @Test
  public void testNonForwardQuery() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
      hAdmin.close();
      if (htable == null) {
        htable.close();
      }
    }
  }

  @Test
  public void testJoin() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
      hAdmin.close();
      if (htable != null) {
        htable.close();
      }
    }
  }

  @Test
  public void testInsertInto() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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
      assertTrue(e.getMessage().indexOf("VerifyException") >= 0);
    } finally {
      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    }
  }

  @Test
  public void testInsertIntoRowField() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
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
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

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

      executeString("DROP TABLE base_table PURGE").close();
      executeString("DROP TABLE hbase_mapped_table PURGE").close();
    } finally {
      if (scanner != null) {
        scanner.close();
      }

      if (htable != null) {
        htable.close();
      }
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
            for (Map.Entry<byte[], byte[]> valueEntry: values.entrySet()) {
              byte[] keyBytes = valueEntry.getKey();
              byte[] valueBytes = valueEntry.getValue();

              if (binaries[i]) {
                sb.append("\"").append(keyBytes == null ? "" : Bytes.toLong(keyBytes)).append("\"");
                sb.append(": \"").append(HBaseBinarySerializerDeserializer.deserialize(schema.getColumn(i), valueBytes)).append("\"");
              } else {
                sb.append("\"").append(keyBytes == null ? "" : new String(keyBytes)).append("\"");
                sb.append(": \"").append(HBaseTextSerializerDeserializer.deserialize(schema.getColumn(i), valueBytes)).append("\"");
              }
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
