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
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.TajoRuntimeException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSchema {
	
	Schema schema;
	Column col1;
	Column col2;
	Column col3;

  public static final Schema nestedSchema1;
  public static final Schema nestedSchema2;
  public static final Schema nestedSchema3;

  static {
    // simple nested schema
    SchemaBuilder builder1 = SchemaBuilder.builder();
    builder1.add(new Column("s1", Type.INT8));

    Schema nestedRecordSchema = SchemaBuilder.builder()
        .add("s2", Type.FLOAT4)
        .add("s3", Type.TEXT)
        .build();

    Column nestedField = new Column("s4", new TypeDesc(nestedRecordSchema));
    builder1.add(nestedField);

    builder1.add(new Column("s5", Type.FLOAT8));
    nestedSchema1 = builder1.build();
    // two level nested schema
    //
    // s1
    //  |- s2
    //  |- s4
    //     |- s3
    //     |- s4
    //  |- s5
    //  |- s8
    //     |- s6
    //     |- s7
    SchemaBuilder builder2 = SchemaBuilder.builder();
    builder2.add(new Column("s1", Type.INT8));

    Schema nestedRecordSchema1 = SchemaBuilder.builder()
        .add("s2", Type.FLOAT4)
        .add("s3", Type.TEXT)
        .build();

    Column nestedField1 = new Column("s4", new TypeDesc(nestedRecordSchema1));
    builder2.add(nestedField1);

    builder2.add(new Column("s5", Type.FLOAT8));

    Schema nestedRecordSchema2 = SchemaBuilder.builder()
        .add("s6", Type.FLOAT4)
        .add("s7", Type.TEXT)
        .build();

    Column nestedField2 = new Column("s8", new TypeDesc(nestedRecordSchema2));
    builder2.add(nestedField2);
    nestedSchema2 = builder2.build();

    // three level nested schema
    //
    // s1
    //  |- s2
    //  |- s3
    //      |- s4
    //      |- s7
    //          |- s5
    //              |- s6
    //      |- s8
    //  |- s9

    SchemaBuilder builder3 = SchemaBuilder.builder();

    builder3.add("s1", Type.INT8);
    builder3.add("s2", Type.INT8);

    SchemaBuilder s5 = SchemaBuilder.builder();
    s5.add("s6", Type.INT8);

    SchemaBuilder s7 = SchemaBuilder.builder();
    s7.add("s5", new TypeDesc(s5.build()));

    SchemaBuilder s3 = SchemaBuilder.builder();
    s3.add("s4", Type.INT8);
    s3.add("s7", new TypeDesc(s7.build()));
    s3.add("s8", Type.INT8);

    builder3.add(new Column("s3", new TypeDesc(s3.build())));
    builder3.add(new Column("s9", Type.INT8));
    nestedSchema3 = builder3.build();
  }

	@Before
	public void setUp() throws Exception {
    SchemaBuilder schemaBld = SchemaBuilder.builder();
		col1 = new Column("name", Type.TEXT);
		schemaBld.add(col1);
		col2 = new Column("age", Type.INT4);
		schemaBld.add(col2);
		col3 = new Column("addr", Type.TEXT);
		schemaBld.add(col3);
    schema = schemaBld.build();
	}

	@Test
	public final void testSchemaSchema() {
		Schema schema2 = SchemaBuilder.builder().addAll(schema.getRootColumns()).build();
		assertEquals(schema, schema2);
	}

	@Test
	public final void testSchemaSchemaProto() {
		Schema schema2 = SchemaFactory.newV1(schema.getProto());
		
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
		Schema schema = SchemaBuilder.builder().build();
		assertFalse(schema.containsByQualifiedName("studentId"));
    Schema schema2 = SchemaBuilder.builder().addAll(schema.getRootColumns()).add("studentId", Type.INT4).build();
		assertTrue(schema2.containsByQualifiedName("studentId"));
	}

	@Test
	public final void testEqualsObject() {
    Schema schema2 = SchemaBuilder.builder()
        .add("name", Type.TEXT)
        .add("age", Type.INT4)
        .add("addr", Type.TEXT)
        .build();

    assertEquals(schema, schema2);
	}

	@Test
	public final void testGetProto() {
		SchemaProto proto = schema.getProto();
		
		assertEquals("name", proto.getFields(0).getName());
		assertEquals("age", proto.getFields(1).getName());
		assertEquals("addr", proto.getFields(2).getName());
	}
	
	@Test
	public final void testClone() throws CloneNotSupportedException {
    Schema schema = SchemaBuilder.builder()
        .add("abc", Type.FLOAT8)
        .add("bbc", Type.FLOAT8)
        .build();

    Schema schema2 = SchemaFactory.newV1(schema.getProto());
	  assertEquals(schema.getProto(), schema2.getProto());
	  assertEquals(schema.getColumn(0), schema2.getColumn(0));
	  assertEquals(schema.size(), schema2.size());
	  
	  Schema schema3 = (Schema) schema.clone();
	  assertEquals(schema.getProto(), schema3.getProto());
    assertEquals(schema.getColumn(0), schema3.getColumn(0));
    assertEquals(schema.size(), schema3.size());
	}
	
	@Test(expected = TajoRuntimeException.class)
	public final void testAddExistColumn() {
    SchemaBuilder.builder()
        .add("abc", Type.FLOAT8)
        .add("bbc", Type.FLOAT8)
        .add("abc", Type.INT4)
        .build();
  }

	@Test
	public final void testJson() {
		Schema schema2 = SchemaFactory.newV1(schema.getProto());
		String json = schema2.toJson();
		Schema fromJson = CatalogGsonHelper.fromJson(json, SchemaLegacy.class);
		assertEquals(schema2, fromJson);
    assertEquals(schema2.getProto(), fromJson.getProto());
	}

  @Test
  public final void testProto() {
    Schema schema2 = SchemaFactory.newV1(schema.getProto());
    SchemaProto proto = schema2.getProto();
    Schema fromProto = SchemaFactory.newV1(proto);
    assertEquals(schema2, fromProto);
  }

  @Test
  public final void testSetQualifier() {
    Schema schema2 = SchemaFactory.newV1(schema.getProto());
    schema2.setQualifier("test1");
    Column column = schema2.getColumn(1);
    assertEquals(1, schema2.getColumnIdByName("age"));
    assertEquals(column, schema2.getColumn("age"));
    assertEquals(column, schema2.getColumn("test1.age"));

    Schema schema3 = SchemaBuilder.builder()
        .add("tb1.col1", Type.INT4)
        .add("col2", Type.INT4)
        .build();
    assertEquals("tb1", schema3.getColumn(0).getQualifier());
    assertEquals("tb1.col1", schema3.getColumn(0).getQualifiedName());
    assertEquals("col1", schema3.getColumn(0).getSimpleName());
    assertEquals("col2", schema3.getColumn(1).getQualifiedName());

    assertEquals(schema3.getColumn(0), schema3.getColumn("col1"));
    assertEquals(schema3.getColumn(0), schema3.getColumn("tb1.col1"));
    assertEquals(schema3.getColumn(1), schema3.getColumn("col2"));
    assertEquals(schema3.getColumn(1), schema3.getColumn("col2"));

    schema3.setQualifier("tb2");
    assertEquals("tb2", schema3.getColumn(0).getQualifier());
    assertEquals("tb2.col1", schema3.getColumn(0).getQualifiedName());
    assertEquals("col1", schema3.getColumn(0).getSimpleName());
    assertEquals("tb2.col2", schema3.getColumn(1).getQualifiedName());

    assertEquals(schema3.getColumn(0), schema3.getColumn("col1"));
    assertEquals(schema3.getColumn(0), schema3.getColumn("tb2.col1"));
    assertEquals(schema3.getColumn(1), schema3.getColumn("col2"));
    assertEquals(schema3.getColumn(1), schema3.getColumn("tb2.col2"));
  }

  @Test
  public void testNestedRecord1() {
    verifySchema(nestedSchema1);
  }

  @Test
  public void testNestedRecord2() {
    verifySchema(nestedSchema2);
  }

  @Test
  public void testNestedRecord3() {
    verifySchema(nestedSchema3);
  }

  @Test
  public void testNestedRecord4() {

    Schema nf2DotNf1 = SchemaBuilder.builder()
        .add("f1", Type.INT8)
        .add("f2", Type.INT8)
        .build();

    Schema nf2DotNf2 = SchemaBuilder.builder()
        .add("f1", Type.INT8)
        .add("f2", Type.INT8)
        .build();

    Schema nf2 = SchemaBuilder.builder()
        .add("f1", Type.INT8)
        .add("nf1", new TypeDesc(nf2DotNf1))
        .add("nf2", new TypeDesc(nf2DotNf2))
        .add("f2", Type.INT8).build();

    Schema root = SchemaBuilder.builder()
        .add("f1", Type.INT8)
        .add("nf1", Type.INT8)
        .add("nf2", new TypeDesc(nf2))
        .add("f2", Type.INT8).build();

    verifySchema(root);
  }

  public static void verifySchema(Schema s1) {
    assertEquals(s1, s1);

    SchemaProto proto = s1.getProto();
    assertEquals("Proto (de)serialized schema is different from the original: ", s1, SchemaFactory.newV1(proto));

    Schema cloned = null;
    try {
      cloned = (Schema) s1.clone();
    } catch (CloneNotSupportedException e) {
      fail("Clone is failed");
    }

    assertEquals("Cloned schema is different from the original one:", s1, cloned);
  }
}
