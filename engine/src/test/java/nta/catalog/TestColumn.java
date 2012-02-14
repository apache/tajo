package nta.catalog;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.engine.json.GsonCreator;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestColumn {	
	static final String FieldName1="f1";
	static final String FieldName2="f2";
	static final String FieldName3="f3";	
	
	static final DataType Type1 = DataType.BYTE;
	static final DataType Type2 = DataType.INT;
	static final DataType Type3 = DataType.LONG;
	
	static final int Len2 = 10;
	static final int Len3 = 12;
	
	Column field1;
	Column field2;
	Column field3;
	
	@Before
	public void setUp() {
		field1 = new Column(FieldName1, DataType.BYTE);
		field2 = new Column(FieldName2, DataType.INT );
		field3 = new Column(FieldName3, DataType.LONG);
	}
	
	@Test
	public final void testFieldType() {
		Column field1 = new Column(FieldName1, Type1);
		Column field2 = new Column(FieldName2, Type2);
		Column field3 = new Column(FieldName3, Type3);
		
		assertEquals(field1.getDataType(), Type1);		
		assertEquals(field2.getDataType(), Type2);
		assertEquals(field3.getDataType(), Type3);		
	}

	@Test
	public final void testGetFieldName() {
		assertEquals(field1.getName(),FieldName1);
		assertEquals(field2.getName(),FieldName2);
		assertEquals(field3.getName(),FieldName3);
	}

	@Test
	public final void testGetFieldType() {
		assertEquals(field1.getDataType(),Type1);
		assertEquals(field2.getDataType(),Type2);
		assertEquals(field3.getDataType(),Type3);
	}
	
	@Test
	public final void testQualifiedName() {
	  Column col = new Column("table_1.id", DataType.INT);
	  
	  assertTrue(col.isQualifiedName());
	  assertEquals("id", col.getColumnName());
	  assertEquals("table_1.id", col.getName());
	  assertEquals("table_1", col.getTableName());
	}

	@Test
	public final void testToSon() {
		Column col = new Column(field1.getProto());
		String json = col.toJSON();
		System.out.println(json);
		Gson gson = GsonCreator.getInstance();
		Column fromJson = gson.fromJson(json, Column.class);
		assertEquals(col.getColumnName(), fromJson.getColumnName());
		assertEquals(col.getDataType(), fromJson.getDataType());
	}
}
