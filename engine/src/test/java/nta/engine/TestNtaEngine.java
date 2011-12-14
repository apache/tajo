package nta.engine;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableInfo;
import nta.catalog.TableMeta;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.storage.CSVFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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

	@Test
	public final void testCreateTable() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.INT);
		
		TableMeta meta = new TableMeta("test1");
		meta.setStorageType(StoreType.MEM);
		meta.setSchema(schema);
		meta.setTableType(TableType.BASETABLE);
		
		assertFalse(engine.existsTable("test1"));
		engine.createTable(meta);		
		assertTrue(engine.existsTable("test1"));
		engine.dropTable("test1");
	}
	
	@Test
	public final void testDropTable() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.INT);
		
		TableMeta meta = new TableMeta("test2");
		meta.setStorageType(StoreType.MEM);
		meta.setSchema(schema);
		meta.setTableType(TableType.BASETABLE);
		engine.createTable(meta);
		
		assertTrue(engine.existsTable("test2"));
		engine.dropTable("test2");
		assertFalse(engine.existsTable("test2"));	
	}
	
	@Test
	public final void testAttchDeteachTable() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMeta();		
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		meta.putOption(CSVFile.DELIMITER, ",");
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,32";
		tuples[1] = "jihoon,29";
		tuples[2] = "jimin,27";
		tuples[3] = "haemi,25";		
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach1", meta, tuples);
		
		engine.attachTable("attach1", new Path(TEST_DIR+"/attach1"));
		assertTrue(engine.existsTable("attach1"));
		
		TableInfo info = engine.getTableInfo("attach1");
		assertEquals("attach1", info.getName());
		assertEquals(",", info.getOption(CSVFile.DELIMITER));
		assertEquals(StoreType.CSV, info.getStoreType());
		assertEquals(TableType.BASETABLE, info.getTableType());
		
		engine.detachTable("attach1");
		assertFalse(engine.existsTable("attach1"));
		
		new File(TEST_DIR+"/attach1").delete();
	}
	
	@Test
	public final void testGetTables() throws Exception {		
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMeta();		
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		meta.putOption(CSVFile.DELIMITER, ",");
		meta.setStartKey(100);
		meta.setEndKey(200);
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,32";
		tuples[1] = "jihoon,29";
		tuples[2] = "jimin,27";
		tuples[3] = "haemi,25";		
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach3", meta, tuples);		
		engine.attachTable("attach3", new Path(TEST_DIR+"/attach3"));
		assertTrue(engine.existsTable("attach3"));
				
		TableMeta meta2 = new TableMeta(meta.getProto());
		meta2.setStartKey(201);
		meta2.setEndKey(300);		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach4", meta2, tuples);
		engine.attachTable("attach4", new Path(TEST_DIR+"/attach4"));
		assertTrue(engine.existsTable("attach4"));
		
		TableMeta meta3 = new TableMeta(meta.getProto());
		meta3.setStartKey(301);
		meta3.setEndKey(400);
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach5", meta3, tuples);
		engine.attachTable("attach5", new Path(TEST_DIR+"/attach5"));
		assertTrue(engine.existsTable("attach5"));
		
		TableInfo [] sorted = engine.getTablesByOrder();
	}
	
	@Test
	public final void testCatalogLoad() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMeta();		
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		meta.putOption(CSVFile.DELIMITER, ",");
		
		String [] tuples = new String[4];
		tuples[0] = "hyunsik,32";
		tuples[1] = "jihoon,29";
		tuples[2] = "jimin,27";
		tuples[3] = "haemi,25";		
		
		EngineTestingUtils.writeCSVTable(TEST_DIR+"/attach6", meta, tuples);
		
		engine.attachTable("attach6", new Path(TEST_DIR+"/attach6"));
		assertTrue(engine.existsTable("attach6"));
		
		TableMeta meta2 = new TableMeta(meta.getProto());
		meta2.setStartKey(201);
		meta2.setEndKey(300);		
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
