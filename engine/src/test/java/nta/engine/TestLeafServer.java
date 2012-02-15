package nta.engine;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.query.SubQueryRequestImpl;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;

/**
 * 
 * @author Hyunsik Choi
 * 
 */
public class TestLeafServer {

  private static Configuration conf;
  private static NtaTestingUtility util;
  private static String TEST_PATH = "target/test-data/TestLeafServer";
  private static StorageManager sm;

  @BeforeClass
  public static void setUp() throws Exception {
    EngineTestingUtils.buildTestDir(TEST_PATH);
    util = new NtaTestingUtility();
    util.startMiniCluster(2);
    conf = util.getConfiguration();
    sm = StorageManager.get(conf);
    QueryIdFactory.reset();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public final void testRequestSubQuery() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("id", DataType.INT);

    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);

    Appender appender = sm.getTableAppender(meta, "table1");
    int tupleNum = 10000;
    Tuple tuple = null;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      tuple.put(0, DatumFactory.createString("abc"));
      tuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    Fragment[] tablets1 = new Fragment[1];
    tablets1[0] = new Fragment("table1_1", status.getPath(), meta, 0, 40000);
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);

    Fragment[] tablets2 = new Fragment[1];
    tablets2[0] = new Fragment("table1_2", status.getPath(), meta, 40000, status.getLen() - 40000);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(1);

    SubQueryRequest req1 = new SubQueryRequestImpl(
        QueryIdFactory.newQueryUnitId(), new ArrayList<Fragment>(
            Arrays.asList(tablets1)), new Path(TEST_PATH, "out").toUri(),
        "select name, id from table1_1 where id > 5100");
    assertNotNull(leaf1.requestSubQuery(req1.getProto()));
    Thread.sleep(1000);
    SubQueryRequest req2 = new SubQueryRequestImpl(
        QueryIdFactory.newQueryUnitId(), new ArrayList<Fragment>(
            Arrays.asList(tablets2)), new Path(TEST_PATH, "out").toUri(),
        "select name, id from table1_2 where id > 5100");
    assertNotNull(leaf2.requestSubQuery(req2.getProto()));
    
    // for the report sending test
    NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
    Collection<InProgressStatus> list = master.getProgressQueries();
    Set<QueryUnitId> reported = new HashSet<QueryUnitId>();
    Set<QueryUnitId> submitted = new HashSet<QueryUnitId>();
    submitted.add(req1.getId());
    submitted.add(req2.getId());

    int i = 0;
    while (i < 10) { // waiting for the report messages 
      Log.info("Waiting for receiving the report messages");
      Thread.sleep(1000);
      list = master.getProgressQueries();
      for (InProgressStatus ips : list) {
        Log.info(ips.toString());
      }
      reported.clear();
      for (InProgressStatus ips : list) {
        reported.add(new QueryUnitId(ips.getId()));
      }
      if (reported.containsAll(submitted))
        break;
      
      i++;
    }
    assertTrue(reported.containsAll(submitted));
  }

  @Test
  public final void testStoreResult() throws IOException, InterruptedException {
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("id", DataType.INT);

    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);

    Appender appender = sm.getTableAppender(meta, "table2");
    int tupleNum = 10000;
    Tuple tuple = null;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      tuple.put(0, DatumFactory.createString("abc"));
      tuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(tuple);
    }
    appender.close();

    Fragment[] frags = sm.split("table2");

    Schema outputSchema = new Schema();
    outputSchema.addColumn("name", DataType.STRING);
    outputSchema.addColumn("id", DataType.INT);
    TableMeta outputMeta = new TableMetaImpl(outputSchema, StoreType.CSV);
    sm.initTableBase(outputMeta, "table120205");
    System.out.println("Table2: " + frags[0]);
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);
    SubQueryRequest req1 = new SubQueryRequestImpl(
        QueryIdFactory.newQueryUnitId(), new ArrayList<Fragment>(
            Arrays.asList(frags)), new Path(TEST_PATH, "out").toUri(),
        "table120205 := select name, id from table2_1 where id > 5100");
    assertNotNull(leaf1.requestSubQuery(req1.getProto()));
    assertNotNull(sm.getTableMeta(sm.getTablePath("table120205")));
    frags = sm.split("table120205");
    SubQueryRequest req2 = new SubQueryRequestImpl(
        QueryIdFactory.newQueryUnitId(), new ArrayList<Fragment>(
            Arrays.asList(frags)), new Path(TEST_PATH, "out").toUri(),
        "table120205 := select name, id from table120205_1");
    assertNotNull(leaf1.requestSubQuery(req2.getProto()));
    
    // for the report sending test
    NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
    Collection<InProgressStatus> list = master.getProgressQueries();
    Set<QueryUnitId> reported = new HashSet<QueryUnitId>();
    Set<QueryUnitId> submitted = new HashSet<QueryUnitId>();
    submitted.add(req1.getId());
    submitted.add(req2.getId());

    int i = 0;
    while (i < 10) { // waiting for the report messages 
      Log.info("Waiting for receiving the report messages");
      Thread.sleep(1000);
      list = master.getProgressQueries();
      reported.clear();
      for (InProgressStatus ips : list) {
        reported.add(new QueryUnitId(ips.getId()));
      }
      if (reported.containsAll(submitted))
        break;
      
      i++;
    }
    assertTrue(reported.containsAll(submitted));
  }
}
