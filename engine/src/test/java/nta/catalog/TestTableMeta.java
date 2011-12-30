package nta.catalog;

import static org.junit.Assert.*;

import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableProto;
import nta.catalog.proto.TableProtos.TableType;
import nta.engine.EngineTestingUtils;

import org.junit.Before;
import org.junit.Test;

public class TestTableMeta {

	TableMeta meta;
	Schema schema;
	
	@Before
	public void setUp() {
		meta = new TableMeta();
		meta.setStorageType(StoreType.CSV);
		meta.setTableType(TableType.BASETABLE);
		schema = new Schema();
		schema.addColumn("name", DataType.BYTE);
		schema.addColumn("addr", DataType.STRING);
		meta.setSchema(schema);
		meta.setStartKey(100);
		meta.setEndKey(200);
	}

	@Test
	public void testTableMetaTableProto() {
	  TableMeta meta1 = new TableMeta();
    meta1.setStorageType(StoreType.CSV);
    meta1.setTableType(TableType.BASETABLE);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    meta1.setStartKey(100);
    meta1.setEndKey(200);
    
    System.out.println(meta1);
    
		TableMeta meta2 = new TableMeta(meta1.getProto());
		
		assertEquals(meta1, meta2);
	}

	@Test
	public void testGetStorageType() {
		assertEquals(StoreType.CSV, meta.getStoreType());
	}

	@Test
	public void testGetTableType() {
		assertEquals(TableType.BASETABLE, meta.getTableType());
	}

	@Test
	public void testSetTableType() {
		meta.setTableType(TableType.CUBE);
		assertEquals(TableType.CUBE, meta.getTableType());
	}

	@Test
	public void testGetSchema() {
		Schema schema2 = new Schema();
		schema2.addColumn("name", DataType.BYTE);
		schema2.addColumn("addr", DataType.STRING);
		
		assertEquals(schema, schema2);
	}

	@Test
	public void testSetSchema() {
		Schema schema2 = new Schema();
		schema2.addColumn("name", DataType.BYTE);
		schema2.addColumn("addr", DataType.STRING);
		schema2.addColumn("age", DataType.INT);
		
		assertNotSame(meta.getSchema(), schema2);
		meta.setSchema(schema2);
		assertEquals(meta.getSchema(), schema2);
	}

	@Test
	public void testEqualsObject() {
		TableMeta meta2 = new TableMeta();
		meta2.setStorageType(StoreType.CSV);
		meta2.setTableType(TableType.BASETABLE);
		Schema schema2 = new Schema();
		schema2.addColumn("name", DataType.BYTE);
		schema2.addColumn("addr", DataType.STRING);
		meta2.setSchema(schema2);
		meta2.setStartKey(100);
    meta2.setEndKey(200);
		
		assertTrue(meta.equals(meta2));
		
		meta2.setTableType(TableType.CUBE);
		assertNotSame(meta, meta2);
	}
	
	@Test
	public void testPutAndGetOptions() {
		meta.putOption("key", "123123");
		assertEquals("123123", meta.getOption("key"));
	}
	
	@Test
	public void testDeleteOption() {
		meta.putOption("key", "123123");
		assertEquals("123123", meta.getOption("key"));
		assertEquals("123123", meta.delete("key"));
		assertNull(meta.getOption("key"));
		
		TableInfo meta2 = new TableInfo(meta.getProto());
		assertNull(meta2.getOption("key"));
	}

	@Test
	public void testGetProto() {
	  TableMeta meta1 = new TableMeta();
    meta1.setStorageType(StoreType.CSV);
    meta1.setTableType(TableType.BASETABLE);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    meta1.setStartKey(100);
    meta1.setEndKey(200);
    
		TableProto proto = meta1.getProto();
		TableMeta newMeta = new TableMeta(proto);
		
		assertTrue(meta1.equals(newMeta));
		assertTrue(100 == newMeta.getStartKey());
		assertTrue(200 == newMeta.getEndKey());
	}
	
	@Test
	public void testWritable() throws Exception {
		TableMeta meta2 = (TableMeta) EngineTestingUtils.testWritable(meta);
		assertTrue(meta.equals(meta2));
	}
	
	@Test
	public void testSchema() {
	  TableMeta meta1 = new TableMeta();
    meta1.setStorageType(StoreType.CSV);
    meta1.setTableType(TableType.BASETABLE);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    meta1.setStartKey(100);
    meta1.setEndKey(200);
    
    TableMeta meta2 = (TableMeta) meta1.clone();
    
    assertEquals(meta1, meta2);
	}
}
