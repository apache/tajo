package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStorageManager {
	private NtaConf conf; 
	private static String TEST_PATH = "target/test-data/TestStorageManager";
	StorageManager sm = null;
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		EngineTestingUtils.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@SuppressWarnings("unused")
  @Test
	public final void testOpen() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);

		TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
		
		Tuple [] tuples = new Tuple[4];
		for(int i=0; i < tuples.length; i++) {
		  tuples[i] = new VTuple(3);
		  tuples[i].put(DatumFactory.createInt(i),
		      DatumFactory.createInt(i+32),
		      DatumFactory.createString("name"+i));
		}
		
		Appender appender = sm.getTableAppender(meta, "table1");
		for(Tuple t : tuples) {
		  appender.addTuple(t);
		}
		appender.close();

		Scanner scanner = sm.getTableScanner("table1");

		int i=0;
		Tuple tuple = null;		
		while((tuple = scanner.next()) != null) {
			i++;
		}
		assertEquals(4,i);
	}
	
	@Test
	public void testGetFileScanner() throws IOException {	  
	  Schema schema = new Schema();
    schema.addColumn("string", DataType.STRING);
    schema.addColumn("int", DataType.INT);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    meta.putOption(CSVFile2.DELIMITER, ",");
    
    sm.initTableBase(meta, "table2");
    Appender appender = sm.getAppender(meta, "table2", System.currentTimeMillis()+"");
    
    int tupleNum = 10000;
    VTuple vTuple = null;
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createString("abc"));
      vTuple.put(1, DatumFactory.createInt(i+1));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table2")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    System.out.println("fileLen: " + fileLen + ", randomNum: " + randomNum);
    
    Fragment[] tablets = new Fragment[1];
    tablets[0] = new Fragment("table2_1", status.getPath(), meta, 0, randomNum);
    
    Scanner fileScanner = sm.getScanner(meta, tablets);
    int tupleCnt = 0;
    while((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
    fileScanner.close();
    
    tablets[0] = new Fragment("table2_2", status.getPath(), meta, randomNum, fileLen - randomNum);

    fileScanner = new CSVFile2.CSVScanner(conf, schema, tablets);
    while((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
    fileScanner.close();    
    
    assertEquals(tupleNum, tupleCnt); 
	}

  @Test
  public final void testInitLocalTable() {

  }
}
