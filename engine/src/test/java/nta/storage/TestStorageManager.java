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
import nta.engine.ipc.protocolrecords.Tablet;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
		Tuple tuple = null;		
		while((tuple = scanner.next()) != null) {
			i++;
		}
		assertEquals(4,i);
	}
	
	@Test
	public void testGetFileScanner() throws IOException {	  
	  FileSystem fs = LocalFileSystem.get(conf);
	  StorageManager sm = new StorageManager(conf, fs);
	  
	  Schema schema = new Schema();
    schema.addColumn("string", DataType.STRING);
    schema.addColumn("int", DataType.INT);
    
    TableMeta meta = new TableMeta();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);
    meta.setTableType(TableType.BASETABLE);
    
    FSDataOutputStream out = fs.create(new Path(TEST_PATH, ".meta"));
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();
    
    Path path = new Path(TEST_PATH);    
    Appender appender = sm.getAppender(meta, path);
    int tupleNum = 10000;
    VTuple vTuple = null;
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, "abc");
      vTuple.put(1, (Integer)(i+1));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(new Path(path, "data/table1.csv"));
    long fileLen = status.getLen();   // 88894
    long randomNum = (long) (Math.random() * fileLen) + 1;
    
    Tablet[] tablets = new Tablet[1];
    Tablet tablet = new Tablet(path, "table1.csv", 0, randomNum);
    tablets[0] = tablet;    
    
    FileScanner fileScanner = sm.getScanner(tablets);
    int tupleCnt = 0;
    while((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
    fileScanner.close();
    
    tablet = new Tablet(path, "table1.csv", randomNum, fileLen - randomNum);
    tablets[0] = tablet;
    fileScanner = new CSVFile2.CSVScanner(conf, schema, tablets);
    while((vTuple = (VTuple) fileScanner.next()) != null) {
      tupleCnt++;
    }
    fileScanner.close();    
    
    assertEquals(tupleNum, tupleCnt); 
	}
}
