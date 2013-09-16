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
import org.junit.Before;
import org.junit.Test;
import org.apache.tajo.catalog.exception.AlreadyExistsFieldException;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.TajoDataTypes.Type;

import static org.junit.Assert.*;

public class TestSchema {
	
	Schema schema;
	Column col1;
	Column col2;
	Column col3;

	@Before
	public void setUp() throws Exception {
		schema = new Schema();
		col1 = new Column("name", Type.TEXT);
		schema.addColumn(col1);
		col2 = new Column("age", Type.INT4);
		schema.addColumn(col2);
		col3 = new Column("addr", Type.TEXT);
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
		assertEquals(col1, schema.getColumnByFQN("name"));
		assertEquals(col2, schema.getColumnByFQN("age"));
		assertEquals(col3, schema.getColumnByFQN("addr"));
	}

	@Test
	public final void testAddField() {
		Schema schema = new Schema();
		assertFalse(schema.contains("studentId"));
		schema.addColumn("studentId", Type.INT4);
		assertTrue(schema.contains("studentId"));
	}

	@Test
	public final void testEqualsObject() {
		Schema schema2 = new Schema();
		schema2.addColumn("name", Type.TEXT);
		schema2.addColumn("age", Type.INT4);
		schema2.addColumn("addr", Type.TEXT);
		
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
	  schema.addColumn("abc", Type.FLOAT8);
	  schema.addColumn("bbc", Type.FLOAT8);
	  
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
    schema.addColumn("abc", Type.FLOAT8);
    schema.addColumn("bbc", Type.FLOAT8);
    schema.addColumn("abc", Type.INT4);
	}

	@Test
	public final void testJson() {
		Schema schema2 = new Schema(schema.getProto());
		String json = schema2.toJson();
		Schema fromJson = CatalogGsonHelper.fromJson(json, Schema.class);
		assertEquals(schema2, fromJson);
    assertEquals(schema2.getProto(), fromJson.getProto());
	}

  @Test
  public final void testProto() {
    Schema schema2 = new Schema(schema.getProto());
    SchemaProto proto = schema2.getProto();
    Schema fromJson = new Schema(proto);
    assertEquals(schema2, fromJson);
  }

  @Test
  public final void testSetQualifier() {
    Schema schema2 = new Schema(schema.getProto());
    schema2.setQualifier("test1");
    Column column = schema2.getColumn(1);
    assertEquals(column, schema2.getColumnByName("age"));
    assertEquals(column, schema2.getColumnByFQN("test1.age"));
  }
}
