package nta.storage;

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
import nta.common.exception.InvalidAddressException;
import nta.common.type.IPv4;
import nta.conf.NtaConf;
import nta.engine.NConstants;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRawFile {
	private NtaConf conf; 
	private static String TEST_PATH = "target/test-data/TestRawFile";
	private File testDir;
	private FileSystem fs;

	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH+"/data");
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
	public final void testInit() throws IOException {
		StorageManager sm = new StorageManager(conf, fs);
		
		Schema schema = new Schema();
		schema.addColumn("id", DataType.LONG);
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.SHORT);
		
		TableMeta meta = new TableMeta("raw1");
		meta.setSchema(schema);
		meta.setStorageType(StoreType.RAW);
		meta.setTableType(TableType.BASETABLE);
		Store store = sm.create(meta);
		
		UpdatableScanner scanner = sm.getUpdatableScanner(store);
		assertNotNull(scanner);
	}

	@Test
	public final void testNext2() throws IOException, InvalidAddressException {
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
		UpdatableScanner scanner = sm.getUpdatableScanner(store);
		
		IPv4[] ips = new IPv4[4];
		for (int i = 1; i <= ips.length; i++) {
			ips[i-1] = new IPv4("163.152.161." + i);
		}
		
		VTuple t1 = new VTuple(3);
		t1.put(0, "hyunsik");
		t1.put(1, (Integer)1);
		t1.put(2, ips[0].getBytes());
		scanner.addTuple(t1);
		
		VTuple t2 = new VTuple(3);
		t2.put(0, "jihoon");
		t2.put(1, (Integer)2);
		t2.put(2, ips[1].getBytes());
		scanner.addTuple(t2);
		
		VTuple t3 = new VTuple(3);
		t3.put(0, "jimin");
		t3.put(1, (Integer)3);
		t3.put(2, ips[2].getBytes());
		scanner.addTuple(t3);
		
		VTuple t4 = new VTuple(3);
		t4.put(0, "haemi");
		t4.put(1, (Integer)4);
		t4.put(2, ips[3].getBytes());
		scanner.addTuple(t4);
		scanner.close();
		
		Scanner readonly = sm.getScanner(store);
		
		Tuple tuple = null;
		tuple = readonly.next();
		assertNotNull(tuple);
		assertEquals("hyunsik",tuple.getString(0));
		assertEquals(1,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[0].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = readonly.next();
		assertNotNull(tuple);
		assertEquals("jihoon",tuple.getString(0));
		assertEquals(2,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[1].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = readonly.next();
		assertNotNull(tuple);
		assertEquals("jimin",tuple.getString(0));
		assertEquals(3,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[2].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = readonly.next();
		assertNotNull(tuple);
		assertEquals("haemi",tuple.getString(0));
		assertEquals(4,tuple.getInt(1));
		assertTrue(Arrays.equals(ips[3].getBytes(), tuple.getIPv4Bytes(2)));
		
		
		readonly.close();
	}
}
