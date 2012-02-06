package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

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
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("blood", DataType.STRING);
    schema.addColumn("country", DataType.STRING);
    schema.addColumn("region", DataType.STRING);
    
    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.RAW);
    
    sm.initTableBase(meta, "table1");
    
    int tupleNum = 10000;
    VTuple tuple = null;
    Appender appender = sm.getAppender(meta, "table1","file1");
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(6);
      tuple.put(0, DatumFactory.createInt((Integer)(i+1)));
      tuple.put(1, DatumFactory.createString("haemi"));
      tuple.put(2, DatumFactory.createInt(25));
      tuple.put(3, DatumFactory.createString("a"));
      tuple.put(4, DatumFactory.createString("korea"));
      tuple.put(5, DatumFactory.createString("sanbon"));
      appender.addTuple(tuple);
    }
    appender.close();
    
    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
  
    Fragment[] tablets = new Fragment[1];
    tablets[0] = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);    
    
    Scanner scanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while ((tuple = (VTuple) scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();    
    
    assertEquals(tupleNum, tupleCnt);
	}
}
