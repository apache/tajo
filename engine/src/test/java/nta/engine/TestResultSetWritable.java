/**
 * 
 */
package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.common.type.IPv4;
import nta.conf.NtaConf;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Store;
import nta.storage.Tuple;
import nta.storage.UpdatableScanner;
import nta.storage.VTuple;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jimin
 * 
 */
public class TestResultSetWritable {

	private NtaConf conf;
	private static String TEST_PATH = "target/test-data/TestResultSetWritable";
	private File testDir;
	private FileSystem fs;
	private ResultSetWritable result;
	private IPv4[] ips;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
		fs = LocalFileSystem.get(conf);

		testDir = new File(TEST_PATH);
		if (testDir.exists()) {
			fs.delete(new Path(TEST_PATH), true);
		}
		testDir.mkdirs();
		Path tablePath = new Path(testDir.getAbsolutePath(), "table1");
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		schema.addColumn("ip", DataType.IPv4);

		TableMeta meta = new TableMeta();
		meta.setName("table1");
		meta.setSchema(schema);
		meta.setStorageType(StoreType.RAW);
		meta.setTableType(TableType.BASETABLE);

		FileSystem fs = LocalFileSystem.get(conf);
		StorageManager sm = new StorageManager(conf, fs);
		Store store = sm.create(meta);
		Appender appender = sm.getAppender(meta, tablePath);

		ips = new IPv4[4];
		for (int i = 1; i <= ips.length; i++) {
			ips[i - 1] = new IPv4("163.152.161." + i);
		}

		VTuple t1 = new VTuple(3);
		t1.put(0, "hyunsik");
		t1.put(1, (Integer) 1);
		t1.put(2, ips[0].getBytes());
		appender.addTuple(t1);

		VTuple t2 = new VTuple(3);
		t2.put(0, "jihoon");
		t2.put(1, (Integer) 2);
		t2.put(2, ips[1].getBytes());
		appender.addTuple(t2);

		VTuple t3 = new VTuple(3);
		t3.put(0, "jimin");
		t3.put(1, (Integer) 3);
		t3.put(2, ips[2].getBytes());
		appender.addTuple(t3);

		VTuple t4 = new VTuple(3);
		t4.put(0, "haemi");
		t4.put(1, (Integer) 4);
		t4.put(2, ips[3].getBytes());
		appender.addTuple(t4);
		appender.close();

		result = new ResultSetWritable();
		result.setResult(tablePath);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		testDir.delete();
		result.close();
	}

	@Test
	public void testNext() throws IOException {
		Tuple tuple = null;
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("hyunsik",tuple.getString(0));
		assertEquals(1,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[0].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("jihoon",tuple.getString(0));
		assertEquals(2,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[1].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("jimin",tuple.getString(0));
		assertEquals(3,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[2].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("haemi",tuple.getString(0));
		assertEquals(4,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[3].getBytes(), tuple.getIPv4Bytes(2)));
	}
}
