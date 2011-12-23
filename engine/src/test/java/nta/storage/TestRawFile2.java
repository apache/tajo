package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.common.exception.InvalidAddressException;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.storage.RawFile2.RawFileAppender;
import nta.storage.RawFile2.RawFileScanner;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRawFile2 {

	private NtaConf conf;
	private static String TEST_PATH = "target/test-data/TestRawFile2";
	private File testDir;
	private FileSystem fs;
	
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH+"/data");
		conf.setInt(NConstants.RAWFILE_SYNC_INTERVAL, 100);
		fs = LocalFileSystem.get(conf);
		
		testDir = new File(TEST_PATH);
		if(testDir.exists()) {
			fs.delete(new Path(TEST_PATH+"/data"), true);
		}
		testDir.mkdirs();
	}
	
	@After
	public void tearDown() throws Exception {
		testDir.delete();
	}
	
	@Test
	public void test() throws IOException, InvalidAddressException {
		Schema schema = new Schema();
		schema.addColumn("id", DataType.INT);
		schema.addColumn("age", DataType.LONG);
		
		Path tablePath = new Path(testDir.getAbsolutePath(), "table1");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(tablePath)) {
			fs.delete(tablePath, true);
		}

		int tupleNum = 10000;
		VTuple tuple = null;
		RawFileAppender appender = RawFile2.getAppender(conf, tablePath, schema);
		for (int i = 0; i < tupleNum; i++) {
			tuple = new VTuple(2);
			tuple.put(0, (Integer)(i+1));
			tuple.put(1, (Long)(i+20l));
			appender.addTuple(tuple);
		}
		appender.close();
		
		Random random = new Random(System.currentTimeMillis());
		FileStatus status = fs.getFileStatus(new Path(tablePath, "data/table.raw"));
		long fileLen = status.getLen();
		long midPos = random.nextInt((int)fileLen);

		int tupleCnt = 0;
		tuple = null;
		RawFileScanner scanner = RawFile2.getScanner(conf, tablePath, schema, 0, midPos);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		--tupleCnt;

		scanner = RawFile2.getScanner(conf, tablePath, schema, midPos, fileLen-midPos);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(--tupleCnt, tupleNum);
	}
}
