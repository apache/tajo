package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.engine.EngineTestingUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStorageManager {
	private NtaConf conf; 
	private static String TEST_PATH = "target/test-data/TestStorageManager";

	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		
		EngineTestingUtils.buildTestDir(TEST_PATH);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public final void testOpen() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);

		TableMeta meta = new TableMeta();
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		meta.putOption(CSVFile.DELIMITER, ",");
		String [] tuples = {
				"1,32,hyunsik",
				"2,29,jihoon",
				"3,28,jimin",
				"4,24,haemi"
		};

		EngineTestingUtils.writeCSVTable(TEST_PATH+"/table1", meta, tuples);

		FileSystem fs = LocalFileSystem.get(conf);
		StorageManager sm = new StorageManager(conf, fs);
		File file = new File(TEST_PATH+"/table1");
		Store store = sm.open(new Path("file:///"+file.getAbsolutePath()).toUri());

		Scanner scanner = sm.getScanner(store);

		int i=0;
		VTuple tuple = null;		
		while((tuple = scanner.next2()) != null) {
			i++;
		}
		assertEquals(4,i);
	}
}
