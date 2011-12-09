package nta.catalog;

import static org.junit.Assert.*;

import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableProto;
import nta.catalog.proto.TableProtos.TableType;
import nta.engine.TestUtils;

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
		TableInfo meta2 = new TableInfo(meta.getProto());
		
		assertEquals(meta, meta2);
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
		
		assertEquals(meta, meta2);
		
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
		TableProto proto = meta.getProto();
		TableInfo newMeta = new TableInfo(proto);
		
		assertEquals(meta, newMeta);
		assertTrue(100 == meta.getStartKey());
		assertTrue(200 == meta.getEndKey());
	}
	
	@Test
	public void testWritable() throws Exception {
		TableMeta meta2 = (TableMeta) TestUtils.testWritable(meta);
		assertEquals(meta, meta2);
	}
}
