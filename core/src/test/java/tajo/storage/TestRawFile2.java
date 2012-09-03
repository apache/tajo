package tajo.storage;

import org.apache.hadoop.fs.FileStatus;
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
import tajo.WorkerTestingUtil;
import tajo.engine.ipc.protocolrecords.Fragment;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestRawFile2 {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestRawFile2";
	private StorageManager sm;
	
	@Before
	public void setUp() throws Exception {
		conf = new TajoConf();
		conf.setInt(ConfVars.RAWFILE_SYNC_INTERVAL.varname, 100);
		WorkerTestingUtil.buildTestDir(TEST_PATH);
		sm = StorageManager.get(conf, TEST_PATH);
	}
		
	@Test
  public void testRawFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "testRawFile");
    Appender appender = sm.getAppender(meta, "testRawFile", "file1");
    int tupleNum = 10000;
    VTuple vTuple;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
    
    FileStatus status = sm.listTableFiles("testRawFile")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    
    Fragment[] tablets = new Fragment[2];
    tablets[0] = new Fragment("testRawFile", status.getPath(), meta, 0, randomNum);
    tablets[1] = new Fragment("testRawFile", status.getPath(), meta, randomNum, (fileLen - randomNum));

    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while (scanner.next() != null) {
      tupleCnt++;
    }
    scanner.close();    
    
    assertEquals(tupleNum, tupleCnt);
	}
	
	@Test
  public void testForSingleFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "testForSingleFile");
    Appender appender = sm.getAppender(meta, "testForSingleFile", "file1");
    int tupleNum = 10000;
    VTuple vTuple;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    // Read a table composed of multiple files
    FileStatus status = sm.listTableFiles("testForSingleFile")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    
    Fragment[] tablets = new Fragment[3];
    tablets[0] = new Fragment("testForSingleFile", status.getPath(), meta, 0, randomNum/2);
    tablets[1] = new Fragment("testForSingleFile", status.getPath(), meta, randomNum/2, (randomNum - randomNum/2));
    tablets[2] = new Fragment("testForSingleFile", status.getPath(), meta, randomNum, (fileLen - randomNum));

    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while (scanner.next() != null) {
      tupleCnt++;
    }
    scanner.close();   
    
    assertEquals(tupleNum, tupleCnt);
  }
  
  @Test
  public void testForMultiFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "testForMultiFile");
    Appender appender = sm.getAppender(meta, "testForMultiFile", "file1");
    int tupleNum = 10000;
    VTuple vTuple;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    appender = sm.getAppender(meta, "testForMultiFile", "file2");
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt(i+1000));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus[] status = sm.listTableFiles("testForMultiFile");
    long fileLen = status[0].getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;

    Fragment[] tablets = new Fragment[4];
    tablets[0] = new Fragment("testForMultiFile", status[0].getPath(), meta, 0, randomNum);
    tablets[1] = new Fragment("testForMultiFile", status[0].getPath(), meta, randomNum, (fileLen - randomNum));
    
    fileLen = status[1].getLen();
    randomNum = (long) (Math.random() * fileLen) + 1;
    tablets[2] = new Fragment("testForMultiFile", status[1].getPath(), meta, 0, randomNum);
    tablets[3] = new Fragment("testForMultiFile", status[1].getPath(), meta, randomNum, (fileLen - randomNum));
    
    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while (scanner.next() != null) {
      tupleCnt++;
    }
    scanner.close();   
    
    assertEquals(tupleNum*2, tupleCnt);
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
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW, options);

    sm.initTableBase(meta, "raw");
    Appender appender = sm.getAppender(meta, "raw", "table.dat");

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

    Scanner scanner = sm.getScanner("raw", "table.dat");
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
  }
}
