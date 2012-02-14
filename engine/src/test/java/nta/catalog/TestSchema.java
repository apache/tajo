package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import nta.catalog.exception.AlreadyExistsFieldException;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.engine.json.GsonCreator;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

public class TestSchema {
	
	Schema schema;
	Column col1;
	Column col2;
	Column col3;

	@Before
	public void setUp() throws Exception {
		schema = new Schema();
		col1 = new Column("name", DataType.STRING);
		schema.addColumn(col1);
		col2 = new Column("age", DataType.INT);
		schema.addColumn(col2);
		col3 = new Column("addr", DataType.STRING);
		schema.addColumn(col3);
	}

	@Test
	public final void testSchemaSchema() {
		Schema schema2 = new Schema(schema);
		
		assertEquals(schema, schema2);
	}

	@Test
	public final void testSchemaSchemaProto() {
		Schema schema2 = new Schema(schema.getProto());
		
		assertEquals(schema, schema2);
	}

	@Test
	public final void testGetColumnString() {
		assertEquals(col1, schema.getColumn("name"));
		assertEquals(col2, schema.getColumn("age"));
		assertEquals(col3, schema.getColumn("addr"));
	}

	@Test
	public final void testAddField() {
		Schema schema = new Schema();
		assertFalse(schema.contains("studentId"));
		schema.addColumn("studentId", DataType.INT);
		assertTrue(schema.contains("studentId"));
	}

	@Test
	public final void testEqualsObject() {
		Schema schema2 = new Schema();
		schema2.addColumn("name", DataType.STRING);
		schema2.addColumn("age", DataType.INT);
		schema2.addColumn("addr", DataType.STRING);
		
		assertEquals(schema, schema2);
	}

	@Test
	public final void testGetProto() {
		SchemaProto proto = schema.getProto();
		
		assertEquals("name", proto.getFields(0).getColumnName());
		assertEquals("age", proto.getFields(1).getColumnName());
		assertEquals("addr", proto.getFields(2).getColumnName());
	}
	
	@Test
	public final void testClone() throws CloneNotSupportedException {
	  Schema schema = new Schema();
	  schema.addColumn("abc", DataType.DOUBLE);
	  schema.addColumn("bbc", DataType.DOUBLE);
	  
	  Schema schema2 = new Schema(schema.getProto());
	  assertEquals(schema.getProto(), schema2.getProto());
	  assertEquals(schema.getColumn(0), schema2.getColumn(0));
	  assertEquals(schema.getColumnNum(), schema2.getColumnNum());
	  
	  Schema schema3 = (Schema) schema.clone();
	  assertEquals(schema.getProto(), schema3.getProto());
    assertEquals(schema.getColumn(0), schema3.getColumn(0));
    assertEquals(schema.getColumnNum(), schema3.getColumnNum());
	}
	
	@Test(expected = AlreadyExistsFieldException.class)
	public final void testAddExistColumn() {
    Schema schema = new Schema();
    schema.addColumn("abc", DataType.DOUBLE);
    schema.addColumn("bbc", DataType.DOUBLE);
    schema.addColumn("abc", DataType.INT);
	}

	@Test
	public final void testJson() {
		Schema schema2 = new Schema(schema.getProto());
		String json = schema2.toJson();
		System.out.println(json);
		Gson gson = GsonCreator.getInstance();
		Schema fromJson = gson.fromJson(json, Schema.class);
		assertEquals(schema2.getProto(), fromJson.getProto());
		assertEquals(schema2.getColumn(0), fromJson.getColumn(0));
		assertEquals(schema2.getColumnNum(), fromJson.getColumnNum());
	}
}
