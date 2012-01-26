package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.exception.NTAQueryException;
import nta.storage.CSVFile2;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

/**
 * @author Hyunsik Choi
 */
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
		
		EngineTestingUtils.buildTestDir(TEST_DIR);
		
		Schema schema = new Schema();
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
		meta.putOption(CSVFile2.DELIMITER, ",");
		
		String [] tuples = {
		"1,32,hyunsik",
		"2,29,jihoon",
		"3,28,jimin",
		"4,24,haemi"
		};
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/table1", meta, tuples);
		
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
		
	public final void testCreateTable() throws IOException {
		assertFalse(engine.existsTable("made"));
		engine.updateQuery(SELECTION_TEST[6]);
		assertTrue(engine.existsTable("made"));
		engine.dropTable("made");
	}
	
	
	public final void testInsertIntoMem() throws IOException {
		engine.updateQuery(SELECTION_TEST[7]);
		assertTrue(engine.existsTable("mod"));
		engine.updateQuery("insert into mod (id, age) values(1,2)");
		engine.updateQuery("insert into mod (id, age) values(2,3)");
		
		ResultSetOld rs1 = engine.executeQuery("select id, age from mod");
		assertTrue(rs1.next());
		assertEquals(1, rs1.getInt("id"));
		assertEquals(2, rs1.getInt("age"));
		
		assertTrue(rs1.next());
		assertEquals(2, rs1.getInt("id"));
		assertEquals(3, rs1.getInt("age"));
		
		assertFalse(rs1.next());
	}
	
	public final void testSelectQuery() throws NTAQueryException {
		ResultSetOld rs1 = engine.executeQuery(SELECTION_TEST[0]);
		
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
	
	public final void testProjectionQuery() throws NTAQueryException {
		ResultSetOld rs1 = engine.executeQuery(SELECTION_TEST[2]);
		
		rs1.next();
		assertEquals(1, rs1.getInt("id"));
		
		rs1.next();
		assertEquals(2, rs1.getInt("id"));
		
		assertFalse(rs1.next());
	}
}
