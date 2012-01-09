package nta.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.engine.query.SubQueryRequestImpl;
import nta.storage.Appender;
import nta.storage.CSVFile2;
import nta.storage.Tuple;
import nta.storage.VTuple;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLeafServer {

  private NtaTestingUtility util;
  private NtaConf conf;
  private static String TEST_PATH = "target/test-data/TestLeafServer";

  @Before
  public void setUp() throws Exception {
    conf = new NtaConf();
    conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
    EngineTestingUtils.buildTestDir(TEST_PATH);

    util = new NtaTestingUtility();
    util.startMiniZKCluster();
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniZKCluster();
  }

  @Test
  public final void testRequestSubQuery() throws IOException {
    util.startMiniNtaEngineCluster(2);

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("id", DataType.INT);

    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);
    Path path = new Path(TEST_PATH);

    FSDataOutputStream out = FileSystem.get(conf).create(
        new Path(path, ".meta"));
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();

    Appender appender = new CSVFile2.CSVAppender(conf, path, schema);
    int tupleNum = 10000;
    Tuple tuple = null;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      tuple.put(0, "abc");
      tuple.put(1, (Integer) (i + 1));
      appender.addTuple(tuple);
    }
    appender.close();

    // the total file size == 88894
    Tablet[] tablets1 = new Tablet[1];
    tablets1[0] = new Tablet(path, "table1.csv", 0, 70000);
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);

    Tablet[] tablets2 = new Tablet[1];
    tablets2[0] = new Tablet(path, "table1.csv", 70000, 10000);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(1);

    SubQueryRequest req = new SubQueryRequestImpl(new ArrayList<Tablet>(
        Arrays.asList(tablets1)), new Path(TEST_PATH, "out").toUri(),
        "select * from test where id > 5100", "test");
    leaf1.requestSubQuery(req);

    SubQueryRequest req2 = new SubQueryRequestImpl(new ArrayList<Tablet>(
        Arrays.asList(tablets2)), new Path(TEST_PATH, "out").toUri(),
        "select * from test where id > 5100", "test");
    leaf2.requestSubQuery(req2);

    leaf1.shutdown("Normally Shutdown");
  }
}
