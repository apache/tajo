/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestColumn {
	static final String FieldName1="f1";
	static final String FieldName2="f2";
	static final String FieldName3="f3";	
	
	static final DataType Type1 = CatalogUtil.newSimpleDataType(Type.BLOB);
	static final DataType Type2 = CatalogUtil.newSimpleDataType(Type.INT4);
	static final DataType Type3 = CatalogUtil.newSimpleDataType(Type.INT8);
	
	Column field1;
	Column field2;
	Column field3;
	
	@Before
	public void setUp() {
		field1 = new Column(FieldName1, Type.BLOB);
		field2 = new Column(FieldName2, Type.INT4);
		field3 = new Column(FieldName3, Type.INT8);
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
	  Column col = new Column("table_1.id", Type.INT4);
	  assertTrue(col.hasQualifier());
	  assertEquals("id", col.getSimpleName());
	  assertEquals("table_1.id", col.getQualifiedName());
	  assertEquals("table_1", col.getQualifier());
	}

  @Test
  public final void testMultiLevelQualifiedName() {
    Column col = new Column("database1.table_1.id", Type.INT4);

    assertTrue(col.hasQualifier());
    assertEquals("id", col.getSimpleName());
    assertEquals("database1.table_1.id", col.getQualifiedName());
    assertEquals("database1.table_1", col.getQualifier());
  }

	@Test
	public final void testToJson() {
		Column col = new Column(field1.getProto());
		String json = col.toJson();
		Column fromJson = CatalogGsonHelper.fromJson(json, Column.class);
		assertEquals(col, fromJson);
	}

  @Test
  public final void testToProto() {
    Column column = new Column(field1.getProto());
    CatalogProtos.ColumnProto proto = column.getProto();
    Column fromProto = new Column(proto);
    assertEquals(column, fromProto);
  }
}
