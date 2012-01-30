package nta.cube;

import java.io.IOException;
import java.util.Random;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.planner.logical.GroupbyNode;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;

/* Constants 묶음 클래스 + 테스트용 data generator */
public class Cons {

  public static int data = 10000;
  public static int totalnodes = 2;
  public static int totalcuboids;
  public static int groupnum;
  public static int measurenum;
  public static String immediatepath;
  public static String datapath;
  public static GroupbyNode gnode;
  public static Schema ORIGIN_SCHEMA;

  public static void datagen() throws IOException {

    
    String TEST_PATH = Cons.datapath;
    EngineTestingUtils.buildTestDir(TEST_PATH);
    
    StorageManager sm = StorageManager.get(new Configuration(), datapath);
    TableMeta meta = new TableMetaImpl();
    meta.setSchema(ORIGIN_SCHEMA);
    meta.setStorageType(StoreType.CSV);

    Appender appender = sm.getTableAppender(meta, "origin");
    int tupleNum = Cons.data;
    VTuple vTuple = null;
    for (int i = 0; i < tupleNum; i++) {
      Random r = new Random();
      vTuple = new VTuple(Cons.ORIGIN_SCHEMA.getColumnNum());
      vTuple.put(0, DatumFactory.createInt(r.nextInt(20)));
      vTuple.put(1, DatumFactory.createInt(65535));
      vTuple.put(2, DatumFactory.createInt(10));
      vTuple.put(3, DatumFactory.createInt(r.nextInt(65535)));
      appender.addTuple(vTuple);
    }
    appender.close();
  }
}
