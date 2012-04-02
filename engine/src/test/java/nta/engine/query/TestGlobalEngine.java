/**
 * 
 */
package nta.engine.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.datum.DatumFactory;
import nta.engine.LeafServer;
import nta.engine.NConstants;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.storage.Appender;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;
import nta.util.FileUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jihoon
 * 
 */
public class TestGlobalEngine {
  private static Log LOG = LogFactory.getLog(TestGlobalEngine.class);

  private static NtaTestingUtility util;
  private static Configuration conf;
  private static CatalogService catalog;
  private static NtaEngineMaster master;
  private static StorageManager sm;

  private String[] query = {
      "select deptname, sum(score) from score group by deptname having sum(score) > 30",
      "select deptname from score",
      "select dept.deptname, score.score from dept,score where score.deptname = dept.deptname",
      "create table test (id int, name string) using csv location '/tmp/data' with ('csv.delimiter'='|')",
      "select dept.deptname, score.score from dept,score where score.deptname = dept.deptname and score.score > 10000"
  };
  private static Map<String, Integer> groupbyResult;
  private static Set<String> scanResult;
  private static Map<String, List<Integer>> joinResult;
  private static Map<String, List<Integer>> selectAfterJoinResult;

  private String tablename;

  @Before
  public void setup() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(3);
    Thread.sleep(2000);
    master = util.getMiniNtaEngineCluster().getMaster();
    conf = util.getConfiguration();
    sm = new StorageManager(conf);

    catalog = master.getCatalog();
    groupbyResult = new HashMap<String, Integer>();
    scanResult = new HashSet<String>();
    joinResult = new HashMap<String, List<Integer>>();
    selectAfterJoinResult = new HashMap<String, List<Integer>>();

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptname", DataType.STRING);
    scoreSchema.addColumn("score", DataType.INT);
    TableMeta scoreMeta = TCatUtil.newTableMeta(scoreSchema, StoreType.CSV);

    Schema deptSchema = new Schema();
    deptSchema.addColumn("id", DataType.INT);
    deptSchema.addColumn("deptname", DataType.STRING);
    TableMeta deptMeta = TCatUtil.newTableMeta(deptSchema, StoreType.CSV);

    Appender appender = sm.getTableAppender(scoreMeta, "score");
    int deptSize = 10000;
    int tupleNum = 10000000;
    Tuple tuple = null;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createString(key));
      tuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(tuple);
      scanResult.add(key);
      if (!groupbyResult.containsKey(key)) {
        groupbyResult.put(key, i + 1);
      } else {
        int n = groupbyResult.get(key);
        groupbyResult.put(key, n + i + 1);
      }
      if (!joinResult.containsKey(key)) {
        List<Integer> list = new ArrayList<Integer>();
        list.add((i+1));
        joinResult.put(key, list);
      } else {
        joinResult.get(key).add((i+1));
      }

      if (i+1 > 10000) {
        if (!selectAfterJoinResult.containsKey(key)) {
          List<Integer> list = new ArrayList<Integer>();
          list.add((i+1));
          selectAfterJoinResult.put(key, list);
        } else {
          selectAfterJoinResult.get(key).add((i+1));
        }
      }
    }
    appender.close();

    TableDesc score = new TableDescImpl("score", scoreSchema, StoreType.CSV,
        new Options(), new Path(conf.get(NConstants.ENGINE_DATA_DIR), "score"));
    catalog.addTable(score);

    appender = sm.getTableAppender(deptMeta, "dept");
    tupleNum = deptSize;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      tuple.put(0, DatumFactory.createInt(i+1));
      tuple.put(1, DatumFactory.createString("test"+i));
      appender.addTuple(tuple);
    }
    appender.close();
    
    TableDesc dept = TCatUtil.newTableDesc("dept", deptMeta, sm.getTablePath("dept"));
    catalog.addTable(dept);
  }

  @After
  public void terminate() throws IOException {
    util.shutdownMiniCluster();
  }

  @Test
  public void testCreateTable() throws Exception {
    String tablename = master.executeQuery(query[3]);
    assertNotNull(tablename);
    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path("/tmp/data/.meta")));
    TableProto proto = TableProto.getDefaultInstance();
    proto = (TableProto) FileUtil.loadProto(conf, new Path("/tmp/data/.meta"), 
        proto);
    TableMeta meta = new TableMetaImpl(proto);
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("name", DataType.STRING);
    assertEquals(schema, meta.getSchema());
    assertEquals(StoreType.CSV, meta.getStoreType());
  }
  
  @Test
  public void testScanQuery() throws Exception {
    String tablename = master.executeQuery(query[1]);
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertTrue(scanResult.contains(deptname));
    }
  }

  @Test
  public void testGroupbyQuery() throws Exception {
    String tablename = master.executeQuery(query[0]);
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertEquals(groupbyResult.get(deptname).intValue(), tuple.get(1).asInt());
    }
  }

  @Test
  public void testJoin() throws Exception {
    String tablename = master.executeQuery(query[2]);
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    Set<Integer> results;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      results = new HashSet<Integer>(joinResult.get(deptname));
      assertTrue(results.contains(tuple.get(1).asInt()));
    }
  }
  
  @Test
  public void testSelectAfterJoin() throws Exception {
    String tablename = master.executeQuery(query[4]);
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    Set<Integer> results;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      results = new HashSet<Integer>(selectAfterJoinResult.get(deptname));
      assertTrue(results.contains(tuple.get(1).asInt()));
    }
  }

//  @Test
  public void testFaultTolerant() throws Exception {
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          LeafServer leaf = util.getMiniNtaEngineCluster().getLeafServer(0);
          LOG.info(">>> " + leaf.getServerName() + " will be halted!!");
          leaf.shutdown(">>> Aborted! <<<");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          tablename = master.executeQuery(query[0]);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    t2.start();
    t1.start();
    t2.join();
    t1.join();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertEquals(groupbyResult.get(deptname).intValue(), tuple.get(1).asInt());
    }
  }
}
