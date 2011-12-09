package nta.catalog;
import static org.junit.Assert.*;

import nta.catalog.Column;
import nta.catalog.proto.TableProtos.AttrType;
import nta.catalog.proto.TableProtos.DataType;
import nta.engine.TestUtils;

import org.junit.Before;
import org.junit.Test;

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
		field1 = new Column(1, FieldName1, DataType.BYTE);
		field2 = new Column(2, FieldName2, DataType.INT );
		field3 = new Column(3, FieldName3, DataType.LONG);
		field3.setAttrType(AttrType.AGGREGATED);
	}
	
	@Test
	public final void testFieldType() {
		Column field1 = new Column(1, FieldName1, Type1);
		Column field2 = new Column(2, FieldName2, Type2);
		Column field3 = new Column(3, FieldName3, Type3);
		
		assertEquals(field1.getDataType(), Type1);		
		assertEquals(field2.getDataType(), Type2);
		assertEquals(field3.getDataType(), Type3);		
	}

	@Test
	public final void testSetFieldId() {
		field1.setId(0);
		assertEquals((Integer)0, field1.getId());
		field1.setId(1);
		assertEquals((Integer)1, field1.getId());
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
	public final void testGetAttrType() {
		assertEquals(AttrType.NORMAL, field1.getAttrType());
		assertEquals(AttrType.NORMAL, field2.getAttrType());
		assertEquals(AttrType.AGGREGATED, field3.getAttrType());
	}
	
	@Test
	public final void testWritable() throws Exception {
		Column t1 = (Column) TestUtils.testWritable(field1);
		assertEquals(field1, t1);
		Column t2 = (Column) TestUtils.testWritable(field2);
		assertEquals(field2, t2);
		Column t3 = (Column) TestUtils.testWritable(field3);
		assertEquals(field3, t3);
	}
}
