package nta.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Fragment;
//import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("blood", DataType.STRING);
    schema.addColumn("country", DataType.STRING);
    schema.addColumn("region", DataType.STRING);
    
    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);
    
    Path path = new Path(TEST_PATH);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1","file1");
    int tupleNum = 10000;
    VTuple vTuple = null;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(6);
      vTuple.put(0, DatumFactory.createInt((Integer)(i+1)));
      vTuple.put(1, DatumFactory.createString("haemi"));
      vTuple.put(2, DatumFactory.createInt(25));
      vTuple.put(3, DatumFactory.createString("a"));
      vTuple.put(4, DatumFactory.createString("korea"));
      vTuple.put(5, DatumFactory.createString("sanbon"));
      appender.addTuple(vTuple);      
    }
    appender.close();

    FileSystem fs = LocalFileSystem.get(conf);
    FileStatus status = fs.getFileStatus(new Path(path, "table1/data/file1"));
    long fileLen = status.getLen();
    long midLen = 288894;

    Fragment[] tablets = new Fragment[1];  
    tablets[0] = new Fragment("frag1", new Path(path, "table1/data/file1"), meta, 0, midLen);
    
    FileScanner fileScanner = new CSVFile2.CSVScanner(conf, schema, tablets);
    int tupleCnt = 0;
    while ((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
    fileScanner.close();
    
    tablets[0] = new Fragment("frag2", new Path(path, "table1/data/file1"), meta, midLen, fileLen - midLen);
    fileScanner = new CSVFile2.CSVScanner(conf, schema, tablets);
    while((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
    fileScanner.close();
    
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
    
    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV); 
    
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