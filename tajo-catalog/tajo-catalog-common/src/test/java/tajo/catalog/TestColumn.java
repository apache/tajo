/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog;

import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.json.GsonCreator;
import tajo.catalog.proto.CatalogProtos.DataType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		assertEquals(field1.getQualifiedName(),FieldName1);
		assertEquals(field2.getQualifiedName(),FieldName2);
		assertEquals(field3.getQualifiedName(),FieldName3);
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
	  
	  assertTrue(col.isQualified());
	  assertEquals("id", col.getColumnName());
	  assertEquals("table_1.id", col.getQualifiedName());
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
