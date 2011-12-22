package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.engine.EngineTestingUtils;
import nta.engine.NConstants;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class TestCSVFile2 {
	
	private NtaConf conf;
	private static String TEST_PATH = "target/test-data/TestCSVFile2";
	
	@Before
	public void setup() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
		EngineTestingUtils.buildTestDir(TEST_PATH);
	}
	
	@Test
	public void test() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("string", DataType.STRING);
		schema.addColumn("int", DataType.INT);
		
		TableMeta meta = new TableMeta();
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		
		Path path = new Path(TEST_PATH);

		Appender appender = new CSVFile2.CSVAppender(conf, path, schema);
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
		FileStatus status = fs.getFileStatus(new Path(path, "data/table1.csv"));
		long fileLen = status.getLen();		// 88894
		long randomNum = (long) (Math.random() * fileLen) + 1;
		
		FileScanner fileScanner = new CSVFile2.CSVScanner(conf, path, schema, 0, randomNum);
		int tupleCnt;
		vTuple = (VTuple) fileScanner.next();
		for (tupleCnt = 1; vTuple != null; tupleCnt++) {
			vTuple = (VTuple) fileScanner.next();			
		}
		fileScanner.close();
		
		fileScanner = new CSVFile2.CSVScanner(conf, path, schema, randomNum, fileLen);
		vTuple = (VTuple) fileScanner.next();
		for (; vTuple != null; tupleCnt++) {
			vTuple = (VTuple) fileScanner.next();
		}		
		fileScanner.close();		
		
		assertEquals(tupleCnt, tupleNum);
	}
}
