package tajo.storage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.WorkerTestingUtil;
import tajo.engine.ipc.protocolrecords.Fragment;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestCSVFile2 { 
  private TajoConf conf;
  StorageManager sm;
  private static String TEST_PATH = "target/test-data/TestCSVFile2";
  
  @Before
  public void setup() throws Exception {
    conf = new TajoConf();
    conf.setVar(ConfVars.ENGINE_DATA_DIR, TEST_PATH);
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);
  }
  
  @Test
  public void testCSVFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("file", DataType.STRING);
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("age", DataType.LONG);
    
    Options options = new Options();
    options.put(CSVFile2.DELIMITER, ",");
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV, options);
    
    Path path = new Path(TEST_PATH);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "file1");
    int tupleNum = 10000;
    VTuple vTuple;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createString("file1"));
      vTuple.put(2, DatumFactory.createString("haemi"));
      vTuple.put(3, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
        
    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    //fileLen: 198894, randomNum: 146657 - Current tupleCnt: 7389
    //fileLen: 198894, randomNum: 139295 - Current tupleCnt: 7021
    //fileLen: 198894, randomNum: 137690 - Current tupleCnt: 6940
    System.out.println("fileLen: " + fileLen + ", randomNum: " + randomNum);
    
    Fragment[] tablets = new Fragment[2];
    tablets[0] = new Fragment("tablet1_1", new Path(path, "table1/data/file1"), meta, 0, randomNum);
    tablets[1] = new Fragment("tablet1_1", new Path(path, "table1/data/file1"), meta, randomNum, (fileLen - randomNum));
    
    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while (scanner.next() != null) {
      tupleCnt++;
    }
    scanner.close();
    
    assertEquals(tupleNum, tupleCnt);
	}
	
  @Test
  public void testVariousTypes() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", DataType.BOOLEAN);
    schema.addColumn("col2", DataType.BYTE);
    schema.addColumn("col3", DataType.CHAR);
    schema.addColumn("col4", DataType.SHORT);
    schema.addColumn("col5", DataType.INT);
    schema.addColumn("col6", DataType.LONG);
    schema.addColumn("col7", DataType.FLOAT);
    schema.addColumn("col8", DataType.DOUBLE);
    schema.addColumn("col9", DataType.STRING);
    schema.addColumn("col10", DataType.BYTES);
    schema.addColumn("col11", DataType.IPv4);
    
    Options options = new Options();
    options.put(CSVFile2.DELIMITER, ",");
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV, options);
    
    sm.initTableBase(meta, "table2");
    Appender appender = sm.getAppender(meta, "table2", "table1.csv");

    Tuple tuple = new VTuple(11);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createByte((byte) 0x99),
        DatumFactory.createChar('7'),
        DatumFactory.createShort((short) 17),
        DatumFactory.createInt(59),
        DatumFactory.createLong(23l),
        DatumFactory.createFloat(77.9f),
        DatumFactory.createDouble(271.9f),
        DatumFactory.createString("hyunsik"),
        DatumFactory.createBytes("hyunsik".getBytes()),
        DatumFactory.createIPv4("192.168.0.1")
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();
    
    Scanner scanner = sm.getScanner("table2", "table1.csv");
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
	}
}