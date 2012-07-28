package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.TableStat;
import nta.conf.NtaConf;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.NConstants;
import nta.engine.WorkerTestingUtil;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Haemi Yang
 *
 */
public class TestSingleCSVFile {
  private NtaConf conf;
  StorageManager sm;
  private static String TEST_PATH = "target/test-data/TestSingleCSVFile";
  private static final Log LOG = LogFactory.getLog(TestSingleCSVFile.class);
  
  @Before
  public void setup() throws Exception {
    conf = new NtaConf();
    conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);
  }
  
  @Test
  public void testSingleCSVFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", DataType.BOOLEAN);
    schema.addColumn("col2", DataType.CHAR);
    schema.addColumn("col3", DataType.SHORT);
    schema.addColumn("col4", DataType.INT);
    schema.addColumn("col5", DataType.LONG);
    schema.addColumn("col6", DataType.FLOAT);
    schema.addColumn("col7", DataType.DOUBLE);
    schema.addColumn("col8", DataType.STRING);
    
    Options options = new Options();
    options.put(CSVFile2.DELIMITER, ",");
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV, options);
    
    sm.initTableBase(meta, "TestCSVTable");
    Appender appender = sm.getAppender(meta, "TestCSVTable", "table1.csv");
    
    VTuple tuple = null;
    int tupleNum = 10000;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(8);
      tuple.put(new Datum[] {
          DatumFactory.createBool(true),
          DatumFactory.createChar('5'),
          DatumFactory.createShort((short) 15),
          DatumFactory.createInt(i),
          DatumFactory.createLong(25l),
          DatumFactory.createFloat(75.5f),
          DatumFactory.createDouble(255.5f),
          DatumFactory.createString("haemiyang"),
      });
      appender.addTuple(tuple);
    }
    appender.close();
    
    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
    
    long fragSize = (long) (Math.random() * stat.getNumBytes().longValue()) + 65536;
    Fragment[] fragments = sm.split("TestCSVTable", fragSize);
    
    int tupleCnt = 0;
    for (int i = 0; i < fragments.length; i++) {
      LOG.info("Fragement Info: " + fragments[i].getStartOffset() + "(start), " + fragments[i].getLength() + "(len)");
      Scanner scanner = sm.getScanner(meta, fragments[i]);
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();
    }
    assertEquals(tupleNum, tupleCnt);
    
//    List<Fragment> fragList = new ArrayList<Fragment>(fragments.length);
//    for (int i = 0; i < fragments.length; i++) {
//      fragList.add(fragments[i]);
//    }
//    
//    int tupleCnt = 0;
//    while (fragments[0] != null) {
//      LOG.info("Fragement Info: " + fragments[0].getStartOffset() + "(start), " + fragments[0].getLength() + "(len)");
//      Scanner scanner = sm.getScanner(meta, fragments);
//      while (scanner.next() != null) {
//        tupleCnt++;
//      }
//      scanner.close();
//      fragList.remove(0);
//      fragments = fragList.toArray(fragments);
//    }
//    assertEquals(tupleNum, tupleCnt);
  }
}
