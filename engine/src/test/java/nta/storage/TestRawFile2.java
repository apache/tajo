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
import nta.engine.ipc.protocolrecords.Tablet;
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
	public void testForSingleFile() throws IOException, InvalidAddressException {
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
		
		Path dataPath = new Path(tablePath, "data/table0.raw");
		Random random = new Random(System.currentTimeMillis());
		FileStatus status = fs.getFileStatus(dataPath);
		long fileLen = status.getLen();
		long midPos = 0;
		while (midPos < 2) {
			midPos = random.nextInt((int)fileLen);
		}

		int tupleCnt = 0;
		tuple = null;
		Tablet[] tablets = new Tablet[1];
		tablets[0] = new Tablet(dataPath, 0, midPos);
		RawFileScanner scanner = RawFile2.getScanner(conf, schema, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		--tupleCnt;
//		System.out.println(tupleCnt);

		tablets = new Tablet[1];
		tablets[0] = new Tablet(dataPath, midPos, fileLen-midPos);
		scanner = RawFile2.getScanner(conf, schema, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum, --tupleCnt);
		
		// Read a table composed of multiple files
		tupleCnt = 0;
		tablets = new Tablet[2];
		tablets[0] = new Tablet(dataPath, 0, midPos);
		tablets[1] = new Tablet(dataPath, midPos, fileLen-midPos);
		scanner = RawFile2.getScanner(conf, schema, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum, --tupleCnt);
		
		tupleCnt = 0;
		tablets = new Tablet[3];
		tablets[0] = new Tablet(dataPath, 0, midPos/2);
		tablets[1] = new Tablet(dataPath, midPos/2, midPos-midPos/2);
		tablets[2] = new Tablet(dataPath, midPos, fileLen-midPos);
		scanner = RawFile2.getScanner(conf, schema, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum, --tupleCnt);
	}
	
	@Test
	public void testForMultiFile() throws IOException {
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
		
		appender = RawFile2.getAppender(conf, tablePath, schema);
		for (int i = 0; i < tupleNum; i++) {
			tuple = new VTuple(2);
			tuple.put(0, (Integer)(i+1));
			tuple.put(1, (Long)(i+20l));
			appender.addTuple(tuple);
		}
		appender.close();
		
		FileStatus[] files = fs.listStatus(new Path(tablePath, "data"));
		Tablet[] tablets = new Tablet[2];
		tablets[0] = new Tablet(files[0].getPath(), 0, files[0].getLen());
		tablets[1] = new Tablet(files[1].getPath(), 0, files[1].getLen());

		RawFile2.RawFileScanner scanner = RawFile2.getScanner(conf, schema, tablets);
		int tupleCnt = 0;
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum*2, --tupleCnt);
	}
}
