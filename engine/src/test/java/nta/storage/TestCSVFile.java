package nta.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.EngineTestingUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCSVFile {
	private NtaConf conf; 
	private static String TEST_PATH = "target/test-data/TestCSVFile";

	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH+"/data");
		EngineTestingUtils.buildTestDir(TEST_PATH);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testNext2() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMeta();
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		meta.putOption(CSVFile.DELIMITER, ",");
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,1";
		tuples[1] = "jihoon,2";
		tuples[2] = "jimin,3";
		tuples[3] = "haemi,4";
		EngineTestingUtils.writeCSVTable(TEST_PATH+"/table1", meta, tuples);
		
		FileSystem fs = LocalFileSystem.get(conf);
		StorageManager sm = new StorageManager(conf, fs);
		File file = new File(TEST_PATH+"/table1");
		Store store = sm.open(new Path("file:///"+file.getAbsolutePath()).toUri());
		Scanner scanner = sm.getScanner(store);
		
		VTuple tuple = null;
		tuple = scanner.next2();
		assertNotNull(tuple);
		assertEquals("hyunsik",tuple.getString(0));
		assertEquals(1,tuple.getInt(1));
		
		tuple = scanner.next2();
		assertNotNull(tuple);
		assertEquals("jihoon",tuple.getString(0));
		assertEquals(2,tuple.getInt(1));
		
		tuple = scanner.next2();
		assertNotNull(tuple);
		assertEquals("jimin",tuple.getString(0));
		assertEquals(3,tuple.getInt(1));
		
		tuple = scanner.next2();
		assertNotNull(tuple);
		assertEquals("haemi",tuple.getString(0));
		assertEquals(4,tuple.getInt(1));
	}
}
