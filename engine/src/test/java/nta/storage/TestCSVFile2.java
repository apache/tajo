package nta.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import nta.catalog.Options;
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
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class TestCSVFile2 { 
  private NtaConf conf;
  StorageManager sm;
  private static String TEST_PATH = "target/test-data/TestCSVFile2";
  
  @Before
  public void setup() throws Exception {
    conf = new NtaConf();
    conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
    EngineTestingUtils.buildTestDir(TEST_PATH);
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
    VTuple vTuple = null;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt((Integer)(i+1)));
      vTuple.put(1, DatumFactory.createString("file1"));
      vTuple.put(2, DatumFactory.createString("haemi"));
      vTuple.put(3, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    
    StatSet statset = appender.getStats();
    assertEquals(tupleNum, statset.getStat(StatType.TABLE_NUM_ROWS).getValue());
        
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
    while ((vTuple = (VTuple) scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();
    
    assertEquals(tupleNum, tupleCnt);
	}
	
  @Test
  public void testVariousTypes() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("image", DataType.BYTES);
    schema.addColumn("flag", DataType.BYTE);
    
    Random rnd = new Random(System.currentTimeMillis());
    
    Options options = new Options();
    options.put(CSVFile2.DELIMITER, ",");
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV, options);
    
    
    sm.initTableBase(meta, "table2");
    Appender appender = sm.getAppender(meta, "table2", "table1.csv");
    
    byte [] image1 = new byte[32];
    
    VTuple vTuple = null;
    vTuple = new VTuple(4);
    vTuple.put(0, DatumFactory.createString("hyunsik"));
    vTuple.put(1, DatumFactory.createInt(33));    
    rnd.nextBytes(image1);
    vTuple.put(2, DatumFactory.createBytes(image1));
    vTuple.put(3, DatumFactory.createByte((byte) 0x09));
    appender.addTuple(vTuple);
    
    byte [] image2 = new byte[32];
    vTuple.clear();
    vTuple.put(0, DatumFactory.createString("jihoon"));
    vTuple.put(1, DatumFactory.createInt(30));
    rnd.nextBytes(image2);
    vTuple.put(2, DatumFactory.createBytes(image2));
    vTuple.put(3, DatumFactory.createByte((byte) 0x12));
    appender.addTuple(vTuple);
    appender.flush();
    appender.close();
    
    Scanner scanner = sm.getScanner("table2", "table1.csv");
    Tuple tuple = scanner.next();    
    assertEquals(DatumFactory.createString("hyunsik"), tuple.get(0));
    assertEquals(DatumFactory.createInt(33), tuple.get(1));
    assertTrue(Arrays.equals(image1, tuple.getBytes(2).asByteArray()));
    assertEquals(0x09, tuple.getByte(3).asByte());
    
    tuple = scanner.next();
    assertEquals(DatumFactory.createString("jihoon"), tuple.get(0));
    assertEquals(DatumFactory.createInt(30), tuple.get(1));
    assertTrue(Arrays.equals(image2, tuple.getBytes(2).asByteArray()));
    assertEquals(0x12, tuple.getByte(3).asByte());
	}
}