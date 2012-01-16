package nta.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.common.exception.InvalidAddressException;
import nta.conf.NtaConf;
import nta.engine.EngineTestingUtils;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
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
	
	@After
	public void tearDown() throws Exception {		
	}
	
	@Test
	public void testForSingleFile() throws IOException, InvalidAddressException {
		Schema schema = new Schema();
		schema.addColumn("id", DataType.INT);
		schema.addColumn("age", DataType.LONG);
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);

		sm.initTableBase(meta, "table1");
		
		int tupleNum = 10000;
		VTuple tuple = null;
		Appender appender = sm.getAppender(meta, "table1","file1");
		for (int i = 0; i < tupleNum; i++) {
			tuple = new VTuple(2);
			tuple.put(0, (Integer)(i+1));
			tuple.put(1, (Long)(i+20l));
			appender.addTuple(tuple);
		}
		appender.close();		
		
		Random random = new Random(System.currentTimeMillis());
		FileStatus status = sm.listTableFiles("table1")[0];
		long fileLen = status.getLen();
		long midPos = 0;
		while (midPos < 2) {
			midPos = random.nextInt((int)fileLen);
		}

		int tupleCnt = 0;
		tuple = null;
		Tablet[] tablets = new Tablet[1];
		tablets[0] = new Tablet("table1_1", status.getPath(), meta, 0, midPos);
		Scanner scanner = sm.getScanner(meta, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		--tupleCnt;
//		System.out.println(tupleCnt);

		tablets = new Tablet[1];
		tablets[0] = new Tablet("table1_2", status.getPath(), meta, midPos, fileLen-midPos);
		scanner = sm.getScanner(meta, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum, --tupleCnt);
		
		// Read a table composed of multiple files
		tupleCnt = 0;
		tablets = new Tablet[2];
		tablets[0] = new Tablet("table1_1", status.getPath(), meta, 0, midPos);
		tablets[1] = new Tablet("table1_2", status.getPath(), meta, midPos, fileLen-midPos);
		scanner = sm.getScanner(meta, tablets);
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum, --tupleCnt);
		
		tupleCnt = 0;
		tablets = new Tablet[3];
		tablets[0] = new Tablet("table1_1", status.getPath(), meta, 0, midPos/2);
		tablets[1] = new Tablet("table1_2", status.getPath(), meta, midPos/2, midPos-midPos/2);
		tablets[2] = new Tablet("table1_3", status.getPath(), meta, midPos, fileLen-midPos);
		scanner = sm.getScanner(meta, tablets);
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
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.RAW);

    sm.initTableBase(meta, "table2");

		int tupleNum = 10000;
		VTuple tuple = null;
		Appender appender = sm.getAppender(meta, "table2", "file1");
		for (int i = 0; i < tupleNum; i++) {
			tuple = new VTuple(2);
			tuple.put(0, (Integer)(i+1));
			tuple.put(1, (Long)(i+20l));
			appender.addTuple(tuple);
		}
		appender.close();		
		
		appender = sm.getAppender(meta, "table2", "file2");
		for (int i = 0; i < tupleNum; i++) {
			tuple = new VTuple(2);
			tuple.put(0, (Integer)(i+1));
			tuple.put(1, (Long)(i+20l));
			appender.addTuple(tuple);
		}
		appender.close();
		
		Tablet[] tablets = sm.split("table2");
		Scanner scanner = sm.getScanner(meta, tablets);
		int tupleCnt = 0;
		do {
			tuple = (VTuple)scanner.next();
			tupleCnt++;
		} while (tuple != null);
		scanner.close();
		
		assertEquals(tupleNum*2, --tupleCnt);
	}
}
