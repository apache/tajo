package nta.cube;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;

/* test용 schema */
public class TestCubeSchema {

  public static int data = 10;
  public static Schema TEST_SCHEMA;
  public static String datapath;

  public static void SetOriginSchema() {
    Schema schema = new Schema();
    // schema.addColumn("sys_uptime", DataType.LONG);//0
    // schema.addColumn("unix_secs", DataType.LONG);
    // schema.addColumn("unix_nsecs", DataType.LONG);
    // schema.addColumn("eng_type", DataType.BYTE);
    // schema.addColumn("end_id", DataType.BYTE);
    // schema.addColumn("itval", DataType.INT);
    //
    // schema.addColumn("srcaddColumnr", DataType.IPv4);
    // schema.addColumn("dstaddColumnr", DataType.IPv4);
    // schema.addColumn("src_net", DataType.INT);
    // schema.addColumn("dst_net", DataType.INT);
    // schema.addColumn("input", DataType.INT);//10
    // schema.addColumn("output", DataType.INT);
    // schema.addColumn("dPkts", DataType.LONG);
    // schema.addColumn("dOctets", DataType.LONG);
    // schema.addColumn("first", DataType.LONG);
    // schema.addColumn("last", DataType.LONG);//15
    // schema.addColumn("srcPort", DataType.INT);
    // schema.addColumn("dstPort", DataType.INT);
    // schema.addColumn("tcp_flags", DataType.BYTE);
    // schema.addColumn("prot", DataType.BYTE);
    // schema.addColumn("tos", DataType.BYTE);//20
    // schema.addColumn("tag", DataType.LONG);
    schema.addColumn("src_net", DataType.INT);
    schema.addColumn("dst_net", DataType.INT);
    schema.addColumn("dst_net2", DataType.INT);
    schema.addColumn("dst_net3", DataType.INT);
    
    TEST_SCHEMA = schema;
  }

  public static void datagen() throws IOException {

    String TEST_PATH = datapath;
    EngineTestingUtils.buildTestDir(TEST_PATH);

    StorageManager sm = StorageManager.get(new Configuration(), datapath);
    TableMeta meta = TCatUtil.newTableMeta(TEST_SCHEMA, StoreType.CSV);

    Appender appender = sm.getTableAppender(meta, "origin");
    int tupleNum = data;
    VTuple vTuple = null;
    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(TEST_SCHEMA.getColumnNum());
      vTuple.put(0, DatumFactory.createInt(i%3));
      vTuple.put(1, DatumFactory.createInt(i));
      vTuple.put(2, DatumFactory.createInt(50-i));
      vTuple.put(3, DatumFactory.createInt(65535));
      appender.addTuple(vTuple);
    }
    appender.close();
  }
}