package nta.engine;

import static org.junit.Assert.*;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.engine.exception.NTAQueryException;
import nta.storage.CSVFile;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQueryEngine {
	private static final String TEST_DIR="target/test-data/TestQueryEngine";
	static NtaConf conf;
	static NtaEngine engine;

	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_BASE_DIR, TEST_DIR);
		conf.setInt(NConstants.MASTER_PORT, 0);
		engine = new NtaEngine(conf);
		engine.init();
		engine.start();
		
		TestUtils.buildTestDir(TEST_DIR);
		
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
		
		TestUtils.writeCSVTable(TEST_DIR+"/table1", meta, tuples);
		
		engine.attachTable("test", new Path(TEST_DIR+"/table1"));
	}
	
	@After
	public void tearDown() throws Exception {
		//TestUtils.cleanTestDir(TEST_DIR);
	}
	
	String [] SELECTION_TEST = {
			"select * from test", // 0
			"select * from test where age > 30", // 1
			"select id from test where age >= 29", // 2
			"select * from test as t1, test as t2 where t1.id = t2.id", // 3
			"select * from test as t1, test as t2 where t1.id = t2.id group by t1.age", // 4
			"select t1.age from test as t1, test as t2 where t1.id = t2.id group by t1.age", // 5
			"create table made (ipv4:src_ip int)", // 6
			"create table mod (id int, age int)", // 7
			"create table mod (id int, age int) using raw" // 8
	};
	
	@Test
	public final void testCreateTable() throws IOException {
		assertFalse(engine.existsTable("made"));
		engine.updateQuery(SELECTION_TEST[6]);
		assertTrue(engine.existsTable("made"));
		engine.dropTable("made");
	}
	
	@Test
	public final void testInsertIntoMem() throws IOException {
		engine.updateQuery(SELECTION_TEST[7]);
		assertTrue(engine.existsTable("mod"));
		engine.updateQuery("insert into mod (id, age) values(1,2)");
		engine.updateQuery("insert into mod (id, age) values(2,3)");
		
		ResultSet rs1 = engine.executeQuery("select id, age from mod");
		assertTrue(rs1.next());
		assertEquals(1, rs1.getInt("id"));
		assertEquals(2, rs1.getInt("age"));
		
		assertTrue(rs1.next());
		assertEquals(2, rs1.getInt("id"));
		assertEquals(3, rs1.getInt("age"));
		
		assertFalse(rs1.next());
	}	
	
	public final void testInsertIntoRaw() throws IOException {
		engine.updateQuery(SELECTION_TEST[8]);
		assertTrue(engine.existsTable("mod"));
		engine.updateQuery("insert into mod (id, age) values(1,2)");
		engine.updateQuery("insert into mod (id, age) values(2,3)");
		
		ResultSet rs1 = engine.executeQuery("select id, age from mod");
		assertTrue(rs1.next());
		assertEquals(1, rs1.getInt("id"));
		assertEquals(2, rs1.getInt("age"));
		
		assertTrue(rs1.next());
		assertEquals(2, rs1.getInt("id"));
		assertEquals(3, rs1.getInt("age"));
		
		assertFalse(rs1.next());
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
	
	@Test
	public final void testProjectionQuery() throws NTAQueryException {
		ResultSet rs1 = engine.executeQuery(SELECTION_TEST[2]);
		
		rs1.next();
		assertEquals(1, rs1.getInt("id"));
		
		rs1.next();
		assertEquals(2, rs1.getInt("id"));
		
		assertFalse(rs1.next());
	}
}
