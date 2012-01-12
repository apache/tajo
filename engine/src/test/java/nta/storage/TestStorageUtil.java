/**
 * 
 */
package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.EngineTestingUtils;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jimin Kim
 * @author Hyunsik Choi
 * 
 */
public class TestStorageUtil {

  private NtaConf conf;
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
    conf = new NtaConf();
    EngineTestingUtils.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);

    schema = new Schema();
    schema.addColumn("string", DataType.STRING);
    schema.addColumn("int", DataType.INT);
    

    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);
    
    Appender appender = sm.getTableAppender(meta, "table1");
    int tupleNum = 10000;

    VTuple vTuple = null;
    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, "abc");
      vTuple.put(1, (Integer)(i+1));
      vTuple.put(1, (Integer) (i + 1));
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
    VTuple vTuple = null;
    while ((vTuple = (VTuple) csvscanner.next()) != null) {
      tupleCnt++;
    }

    assertEquals(tupleNum, tupleCnt);
  }

}
