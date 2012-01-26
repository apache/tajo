package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.storage.CSVFile2;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNtaEngine {
	static NtaConf conf;
	static NtaEngine engine;
	
	static final String TEST_DIR="target/test-data/TestNtaEngine/";
	static final String TEST_BASEDIR=TEST_DIR+"/base";

	@BeforeClass
	public static void setUp() throws Exception {
		conf = new NtaConf();
		conf.setInt(NConstants.MASTER_PORT, 0);
		conf.set(NConstants.ENGINE_BASE_DIR, TEST_BASEDIR);
		
		EngineTestingUtils.buildTestDir(TEST_DIR);
		
		engine = new NtaEngine(conf);
		engine.init();
		engine.start();
	}

	@AfterClass
	public static void tearDown() throws Exception {
			
	}

	public final void testCreateTable() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.INT);
		
		TableMeta info = new TableMetaImpl(schema, StoreType.MEM);		
		TableDesc desc = new TableDescImpl("test1", info);
		
		assertFalse(engine.existsTable("test1"));
		engine.createTable(desc);		
		assertTrue(engine.existsTable("test1"));
		engine.dropTable("test1");
	}
	
	public final void testDropTable() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.INT);
		
		TableMeta info = new TableMetaImpl(schema, StoreType.MEM);
		TableDescImpl meta = new TableDescImpl("test2");
		engine.createTable(meta);
		
		assertTrue(engine.existsTable("test2"));
		engine.dropTable("test2");
		assertFalse(engine.existsTable("test2"));	
	}
	
	@Test
	public final void testAttchDeteachTable() throws Exception {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
		meta.putOption(CSVFile2.DELIMITER, ",");
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,32";
		tuples[1] = "jihoon,29";
		tuples[2] = "jimin,27";
		tuples[3] = "haemi,25";		
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach1", meta, tuples);
		
		engine.attachTable("attach1", new Path(TEST_DIR+"/attach1"));
		assertTrue(engine.existsTable("attach1"));
		
		TableDesc info = engine.getTableDesc("attach1");
		assertEquals("attach1", info.getId());
		assertEquals(",", info.getMeta().getOption(CSVFile2.DELIMITER));
		assertEquals(StoreType.CSV, info.getMeta().getStoreType());
		
		engine.detachTable("attach1");
		assertFalse(engine.existsTable("attach1"));
		
		new File(TEST_DIR+"/attach1").delete();
	}
	
	@Test
	public final void testGetTables() throws Exception {		
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
		meta.putOption(CSVFile2.DELIMITER, ",");
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,32";
		tuples[1] = "jihoon,29";
		tuples[2] = "jimin,27";
		tuples[3] = "haemi,25";		
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach3", meta, tuples);
		engine.attachTable("attach3", new Path(TEST_DIR+"/attach3"));
		assertTrue(engine.existsTable("attach3"));
				
		TableMeta meta2 = new TableMetaImpl(meta.getProto());
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach4", meta2, tuples);
		engine.attachTable("attach4", new Path(TEST_DIR+"/attach4"));
		assertTrue(engine.existsTable("attach4"));
		
		TableMeta meta3 = new TableMetaImpl(meta.getProto());
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach5", meta3, tuples);
		engine.attachTable("attach5", new Path(TEST_DIR+"/attach5"));
		assertTrue(engine.existsTable("attach5"));
	}
	
	// TODO - to be fixed
	public final void testCatalogLoad() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);		
		meta.putOption(CSVFile2.DELIMITER, ",");
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,32";
		tuples[1] = "jihoon,29";
		tuples[2] = "jimin,27";
		tuples[3] = "haemi,25";		
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach6", meta, tuples);
		
		engine.attachTable("attach6", new Path(TEST_DIR+"/attach6"));
		assertTrue(engine.existsTable("attach6"));
		
		TableMeta meta2 = new TableMetaImpl(meta.getProto());
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach7", meta2, tuples);
		engine.attachTable("attach7", new Path(TEST_DIR+"/attach7"));
		assertTrue(engine.existsTable("attach7"));		
		engine.shutdown();
		
		engine = new NtaEngine(conf);
		engine.init();
		engine.start();
		assertTrue(engine.existsTable("attach6"));
		assertTrue(engine.existsTable("attach7"));
	}
}
