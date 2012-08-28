/**
 * 
 */
package tajo.storage;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.DatumFactory;
import tajo.engine.WorkerTestingUtil;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Jimin Kim
 * @author Hyunsik Choi
 * 
 */
public class TestStorageUtil {

  private TajoConf conf;
  private static String TEST_PATH = "target/test-data/TestStorageUtil";

  int tupleNum = 10000;
  Schema schema = null;
  Path path = null;
  
  StorageManager sm;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    conf = new TajoConf();
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);

    schema = new Schema();
    schema.addColumn("string", DataType.STRING);
    schema.addColumn("int", DataType.INT);
    

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    
    Appender appender = sm.getTableAppender(meta, "table1");
    int tupleNum = 10000;

    VTuple vTuple = null;
    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createString("abc"));
      vTuple.put(1, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(vTuple);
    }
    appender.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() throws IOException {    
    Scanner csvscanner =
        sm.getTableScanner("table1");

    int tupleCnt = 0;
    @SuppressWarnings("unused")
    VTuple vTuple = null;
    while ((vTuple = (VTuple) csvscanner.next()) != null) {
      tupleCnt++;
    }

    assertEquals(tupleNum, tupleCnt);
  }

}
