package nta.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.util.FileUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

public class TestStorageManager {
	private NtaConf conf; 
	private static String TEST_PATH = "target/test-data/TestStorageManager";
	private File testDir;

	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		
		testDir = new File(TEST_PATH);
		if(testDir.exists()) {
			testDir.delete();
		}
		testDir.mkdirs();
	}

	@After
	public void tearDown() throws Exception {
		testDir.delete();
	}
	
	public static final void writeSampleCSVTable(String tableDir) throws IOException {
		File tableRoot = new File(tableDir);
		tableRoot.mkdir();
		
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMeta();
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		meta.putOption(CSVFile.DELIMITER, ",");
		
		File metaFile = new File(TEST_PATH+"/table1/.meta");
		FileUtils.writeProto(metaFile, meta.getProto());
		
		File dataDir = new File(TEST_PATH+"/table1/data");
		dataDir.mkdir();
		
		FileWriter writer = new FileWriter(
			new File(TEST_PATH+"/table1/data/split1.csv"));
		writer.write("hyunsik,1\n");
		writer.write("jihoon,2\n");
		writer.write("jimin,3\n");
		writer.write("haemi,4\n");
		writer.close();
	}
	
	public final void testOpen() throws IOException {
		writeSampleCSVTable(TEST_PATH+"/table2");
		
		FileSystem fs = LocalFileSystem.get(conf);
		StoreManager sm = new StoreManager(conf, fs);
		File file = new File(TEST_PATH+"/table1");
		Store store = sm.open(new Path("file:///"+file.getAbsolutePath()).toUri());
		
		Scanner scanner = sm.getScanner(store);
		
		VTuple tuple = null;
		while((tuple = scanner.next2()) != null) {
			System.out.println(tuple);
		}
	}
}
