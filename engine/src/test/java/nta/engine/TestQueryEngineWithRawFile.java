package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.engine.exception.NTAQueryException;
import nta.storage.VTuple;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQueryEngineWithRawFile {
	private static final String TEST_DIR="target/test-data/TestQueryEngineWithRawFile";
	static NtaConf conf;
	static NtaEngine engine;

	@BeforeClass
	public static void setUp() throws Exception {
		EngineTestingUtils.buildTestDir(TEST_DIR);
		
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_BASE_DIR, TEST_DIR);
		//conf.set(NtaEngineConstants.ENGINE_DATA_DIR, TEST_DIR);
		
		engine = new NtaEngine(conf);
		engine.init();
		engine.start();
		
		Schema schema = new Schema();
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);
		
		TableMeta meta = new TableMeta();
		meta.setName("table1");
		meta.setSchema(schema);
		meta.setStorageType(StoreType.RAW);
		meta.setTableType(TableType.BASETABLE);
		
		VTuple [] tuples = new VTuple[4];
		
		tuples[0] = new VTuple(3);
		tuples[0].put(0, 1);
		tuples[0].put(1, 32);
		tuples[0].put(2, "hyunsik");
		tuples[1] = new VTuple(3);
		tuples[1].put(0, 2);
		tuples[1].put(1, 29);
		tuples[1].put(2, "jihoon");
		tuples[2] = new VTuple(3);
		tuples[2].put(0, 3);
		tuples[2].put(1, 28);
		tuples[2].put(2, "jimin");
		tuples[3] = new VTuple(3);
		tuples[3].put(0, 4);
		tuples[3].put(1, 24);
		tuples[3].put(2, "haemi");
		
		FileSystem fs = LocalFileSystem.get(conf);
		fs.delete(new Path(TEST_DIR + "/table1"), true);
		EngineTestingUtils.writeRawTable(conf, meta, tuples);
		
		engine.attachTable("table1", new Path(TEST_DIR+"/data/table1"));
	}
	
	String [] SELECTION_TEST = {
			"select * from table1",
			"select * from table1 where age > 30",
			"select id from table1 where age >= 29",
			"select * from table1 as t1, table1 as t2 where t1.id = t2.id",
			"select * from table1 as t1, table1 as t2 where t1.id = t2.id group by t1.age",
			"select t1.age from table1 as t1, table1 as t2 where t1.id = t2.id group by t1.age",
			"create table made (ipv4:src_ip int)" // 6
	};
	
	@Test
	public final void testCreateTable() throws IOException {
		assertFalse(engine.existsTable("made"));
		engine.updateQuery(SELECTION_TEST[6]);
		assertTrue(engine.existsTable("made"));
	}
	
	@Test
	public final void testSelectQuery() throws NTAQueryException {
		ResultSet rs1 = engine.executeQuery(SELECTION_TEST[0]);
		
		rs1.next();
		assertEquals(1, rs1.getInt("id"));
		assertEquals(32, rs1.getInt("age"));
		
		rs1.next();
		assertEquals(2, rs1.getInt("id"));
		assertEquals(29, rs1.getInt("age"));
		
		rs1.next();
		assertEquals(3, rs1.getInt("id"));
		assertEquals(28, rs1.getInt("age"));
		
		rs1.next();
		assertEquals(4, rs1.getInt("id"));
		assertEquals(24, rs1.getInt("age"));
		
		assertFalse(rs1.next());
		
		rs1 = engine.executeQuery(SELECTION_TEST[1]);
		
		rs1.next();
		assertEquals(1, rs1.getInt("id"));
		assertEquals(32, rs1.getInt("age"));
	
		assertFalse(rs1.next());
	}
	
}
