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
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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
  public void test() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("string", DataType.STRING);
    schema.addColumn("int", DataType.INT);
    
    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);    
    
    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    int tupleNum = 10000;
    VTuple vTuple = null;
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, "abc");
      vTuple.put(1, (Integer)(i+1));
      appender.addTuple(vTuple);
    }
    appender.close();

		FileSystem fs = LocalFileSystem.get(conf);
		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();		// 88894
		long randomNum = (long) (Math.random() * fileLen) + 1;
		
		Tablet[] tablets = new Tablet[1];
		Tablet tablet = new Tablet(status.getPath(), 0, randomNum);
		tablets[0] = tablet;
		
		FileScanner fileScanner = new CSVFile2.CSVScanner(conf, schema, tablets);
		int tupleCnt = 0;
		while((vTuple = (VTuple) fileScanner.next()) != null) {
		  tupleCnt++;
		}
		fileScanner.close();
		
		tablet = new Tablet(status.getPath(), randomNum, fileLen - randomNum);
		tablets[0] = tablet;
		fileScanner = new CSVFile2.CSVScanner(conf, schema, tablets);
    while((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
		fileScanner.close();		
		
		assertEquals(tupleNum, tupleCnt);
	}
}
