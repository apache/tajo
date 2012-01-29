package nta.engine;

import java.util.ArrayList;
import java.util.Arrays;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
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
    util.startMiniZKCluster();
    util.startCatalogCluster();
    util.startMiniNtaEngineCluster(2);
    conf = util.getConfiguration();
    sm = StorageManager.get(conf, TEST_PATH);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniZKCluster();
    util.shutdownMiniNtaEngineCluster();
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
    tablets1[0] = new Fragment("table1_1", status.getPath(), meta, 0, 70000);
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);

    Fragment[] tablets2 = new Fragment[1];
    tablets2[0] = new Fragment("table1_2", status.getPath(), meta, 70000, 10000);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(1);

    SubQueryRequest req = new SubQueryRequestImpl(0, new ArrayList<Fragment>(
        Arrays.asList(tablets1)), new Path(TEST_PATH, "out").toUri(),
        "select * from table1_1 where id > 5100");
    leaf1.requestSubQuery(req.getProto());

    SubQueryRequest req2 = new SubQueryRequestImpl(1, new ArrayList<Fragment>(
        Arrays.asList(tablets2)), new Path(TEST_PATH, "out").toUri(),
        "select * from table1_2 where id > 5100");
    leaf2.requestSubQuery(req2.getProto());

    leaf1.shutdown("Normally Shutdown");
  }
}
