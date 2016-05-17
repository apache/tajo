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

package org.apache.tajo.storage;


import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.util.BytesUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLazyTuple {

  Schema schema;
  byte[][] textRow;
  byte[] nullbytes;
  SerializerDeserializer serde;

  @Before
  public void setUp() {
    nullbytes = "\\N".getBytes();

    schema = SchemaBuilder.builder()
        .add("col1", TajoDataTypes.Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.CHAR, 7))
        .add("col3", TajoDataTypes.Type.INT2)
        .add("col4", TajoDataTypes.Type.INT4)
        .add("col5", TajoDataTypes.Type.INT8)
        .add("col6", TajoDataTypes.Type.FLOAT4)
        .add("col7", TajoDataTypes.Type.FLOAT8)
        .add("col8", TajoDataTypes.Type.TEXT)
        .add("col9", TajoDataTypes.Type.BLOB)
        .add("col10", TajoDataTypes.Type.INT4)
        .add("col11", TajoDataTypes.Type.NULL_TYPE)
        .build();

    StringBuilder sb = new StringBuilder();
    sb.append(DatumFactory.createBool(true)).append('|');
    sb.append(DatumFactory.createChar("str")).append('|');
    sb.append(DatumFactory.createInt2((short) 17)).append('|');
    sb.append(DatumFactory.createInt4(59)).append('|');
    sb.append(DatumFactory.createInt8(23l)).append('|');
    sb.append(DatumFactory.createFloat4(77.9f)).append('|');
    sb.append(DatumFactory.createFloat8(271.9f)).append('|');
    sb.append(DatumFactory.createText("str2")).append('|');
    sb.append(DatumFactory.createBlob("jinho")).append('|');
    sb.append(new String(nullbytes)).append('|');
    sb.append(NullDatum.get());
    textRow = BytesUtils.splitPreserveAllTokens(sb.toString().getBytes(), '|', 13);
    serde = new TextSerializerDeserializer();
    serde.init(schema);
  }

  @Test
  public void testGetDatum() {

    LazyTuple t1 = new LazyTuple(schema, textRow, -1, nullbytes, serde);
    assertEquals(DatumFactory.createBool(true), t1.get(0));
    assertEquals(DatumFactory.createChar("str"), t1.get(1));
    assertEquals(DatumFactory.createInt2((short) 17), t1.get(2));
    assertEquals(DatumFactory.createInt4(59), t1.get(3));
    assertEquals(DatumFactory.createInt8(23l), t1.get(4));
    assertEquals(DatumFactory.createFloat4(77.9f), t1.get(5));
    assertEquals(DatumFactory.createFloat8(271.9f), t1.get(6));
    assertEquals(DatumFactory.createText("str2"), t1.get(7));
    assertEquals(DatumFactory.createBlob("jinho".getBytes()), t1.get(8));
    assertEquals(NullDatum.get(), t1.get(9));
    assertEquals(NullDatum.get(), t1.get(10));
  }

  @Test
  public void testContain() {
    int colNum = schema.size();

    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);
    t1.put(0, DatumFactory.createInt4(1));
    t1.put(3, DatumFactory.createInt4(1));
    t1.put(7, DatumFactory.createInt4(1));

    assertTrue(t1.contains(0));
    assertFalse(t1.contains(1));
    assertFalse(t1.contains(2));
    assertTrue(t1.contains(3));
    assertFalse(t1.contains(4));
    assertFalse(t1.contains(5));
    assertFalse(t1.contains(6));
    assertTrue(t1.contains(7));
    assertFalse(t1.contains(8));
    assertFalse(t1.contains(9));
    assertFalse(t1.contains(10));
  }

  @Test
  public void testPut() {
    int colNum = schema.size();
    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);
    t1.put(0, DatumFactory.createText("str"));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(10, DatumFactory.createFloat4(0.76f));

    assertTrue(t1.contains(0));
    assertTrue(t1.contains(1));

    assertEquals(t1.getText(0), "str");
    assertEquals(t1.get(1).asInt4(), 2);
    assertTrue(t1.get(10).asFloat4() == 0.76f);
  }

  @Test
  public void testEquals() {
    int colNum = schema.size();
    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);
    LazyTuple t2 = new LazyTuple(schema, new byte[colNum][], -1);

    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));

    t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(3, DatumFactory.createInt4(2));

    assertEquals(t1, t2);

    Tuple t3 = new VTuple(colNum);
    t3.put(0, DatumFactory.createInt4(1));
    t3.put(1, DatumFactory.createInt4(2));
    t3.put(3, DatumFactory.createInt4(2));
    assertEquals(t1, t3);
    assertEquals(t2, t3);

    LazyTuple t4 = new LazyTuple(schema, new byte[colNum][], -1);
    assertNotSame(t1, t4);
  }

  @Test
  public void testHashCode() {
    int colNum = schema.size();
    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);
    LazyTuple t2 = new LazyTuple(schema, new byte[colNum][], -1);

    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("str"));

    t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(3, DatumFactory.createInt4(2));
    t2.put(4, DatumFactory.createText("str"));

    assertEquals(t1.hashCode(), t2.hashCode());

    Tuple t3 = new VTuple(colNum);
    t3.put(0, DatumFactory.createInt4(1));
    t3.put(1, DatumFactory.createInt4(2));
    t3.put(3, DatumFactory.createInt4(2));
    t3.put(4, DatumFactory.createText("str"));
    assertEquals(t1.hashCode(), t3.hashCode());
    assertEquals(t2.hashCode(), t3.hashCode());

    Tuple t4 = new VTuple(5);
    t4.put(0, DatumFactory.createInt4(1));
    t4.put(1, DatumFactory.createInt4(2));
    t4.put(4, DatumFactory.createInt4(2));

    assertNotSame(t1.hashCode(), t4.hashCode());
  }

  @Test
  public void testInvalidNumber() {
    byte[][] bytes = BytesUtils.splitPreserveAllTokens(" 1| |2 ||".getBytes(), '|', 5);
    Schema schema = SchemaBuilder.builder()
        .add("col1", TajoDataTypes.Type.INT2)
        .add("col2", TajoDataTypes.Type.INT4)
        .add("col3", TajoDataTypes.Type.INT8)
        .add("col4", TajoDataTypes.Type.FLOAT4)
        .add("col5", TajoDataTypes.Type.FLOAT8)
        .build();

    LazyTuple tuple = new LazyTuple(schema, bytes, 0);
    assertEquals(bytes.length, tuple.size());

    for (int i = 0; i < tuple.size(); i++){
      assertEquals(NullDatum.get(), tuple.get(i));
    }
  }

  @Test
  public void testClone() throws CloneNotSupportedException {
    int colNum = schema.size();
    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);

    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("str"));

    LazyTuple t2 = (LazyTuple) t1.clone();
    assertNotSame(t1, t2);
    assertEquals(t1, t2);

    assertSame(t1.get(4), t2.get(4));

    t1.clear();
    assertFalse(t1.equals(t2));
  }
}
