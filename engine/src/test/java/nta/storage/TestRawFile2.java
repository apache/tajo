package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.StatSet;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.NConstants;
import nta.engine.TCommonProtos.StatType;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Before;
import org.junit.Test;

public class TestRawFile2 {
	private NtaConf conf;
	private static String TEST_PATH = "target/test-data/TestRawFile2";
	private StorageManager sm;
	
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.setInt(NConstants.RAWFILE_SYNC_INTERVAL, 100);
		EngineTestingUtils.buildTestDir(TEST_PATH);
		sm = StorageManager.get(conf, TEST_PATH);
	}
		
	@Test
  public void testRawFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "file1");
    int tupleNum = 10000;
    VTuple vTuple = null;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt((Integer)(i+1)));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    StatSet statset = appender.getStats();
    assertEquals(tupleNum, statset.getStat(StatType.TABLE_NUM_ROWS).getValue());
    
    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    
    Fragment[] tablets = new Fragment[2];
    tablets[0] = new Fragment("tablet1_1", status.getPath(), meta, 0, randomNum);
    tablets[1] = new Fragment("tablet1_2", status.getPath(), meta, randomNum, (fileLen - randomNum));

    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while ((vTuple = (VTuple) scanner.next()) != null) {
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
    
    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "file1");
    int tupleNum = 10000;
    VTuple vTuple = null;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt((Integer)(i+1)));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    // Read a table composed of multiple files
    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    
    Fragment[] tablets = new Fragment[3];
    tablets[0] = new Fragment("tablet1_1", status.getPath(), meta, 0, randomNum/2);
    tablets[1] = new Fragment("tablet1_2", status.getPath(), meta, randomNum/2, (randomNum - randomNum/2));
    tablets[2] = new Fragment("tablet1_2", status.getPath(), meta, randomNum, (fileLen - randomNum));

    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while ((vTuple = (VTuple) scanner.next()) != null) {
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
    
    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "file1");
    int tupleNum = 10000;
    VTuple vTuple = null;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt((Integer)(i+1)));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    appender = sm.getAppender(meta, "table1", "file2");
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt((Integer)(i+1000)));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus[] status = sm.listTableFiles("table1");
    long fileLen = status[0].getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;

    Fragment[] tablets = new Fragment[4];
    tablets[0] = new Fragment("tablet1_1", status[0].getPath(), meta, 0, randomNum);
    tablets[1] = new Fragment("tablet1_2", status[0].getPath(), meta, randomNum, (fileLen - randomNum));
    
    fileLen = status[1].getLen();
    randomNum = (long) (Math.random() * fileLen) + 1;
    tablets[2] = new Fragment("tablet1_2", status[1].getPath(), meta, 0, randomNum);
    tablets[3] = new Fragment("tablet1_2", status[1].getPath(), meta, randomNum, (fileLen - randomNum));
    
    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while ((vTuple = (VTuple) scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();   
    
    assertEquals(tupleNum*2, tupleCnt);
  }
}
