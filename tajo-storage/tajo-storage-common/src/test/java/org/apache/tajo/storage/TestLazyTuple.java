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


import org.apache.tajo.catalog.Schema;
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

    schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.BOOLEAN);
    schema.addColumn("col2", TajoDataTypes.Type.BIT);
    schema.addColumn("col3", TajoDataTypes.Type.CHAR, 7);
    schema.addColumn("col4", TajoDataTypes.Type.INT2);
    schema.addColumn("col5", TajoDataTypes.Type.INT4);
    schema.addColumn("col6", TajoDataTypes.Type.INT8);
    schema.addColumn("col7", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col8", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col9", TajoDataTypes.Type.TEXT);
    schema.addColumn("col10", TajoDataTypes.Type.BLOB);
    schema.addColumn("col11", TajoDataTypes.Type.INET4);
    schema.addColumn("col12", TajoDataTypes.Type.INT4);
    schema.addColumn("col13", TajoDataTypes.Type.NULL_TYPE);

    StringBuilder sb = new StringBuilder();
    sb.append(DatumFactory.createBool(true)).append('|');
    sb.append(new String(DatumFactory.createBit((byte) 0x99).asTextBytes())).append('|');
    sb.append(DatumFactory.createChar("str")).append('|');
    sb.append(DatumFactory.createInt2((short) 17)).append('|');
    sb.append(DatumFactory.createInt4(59)).append('|');
    sb.append(DatumFactory.createInt8(23l)).append('|');
    sb.append(DatumFactory.createFloat4(77.9f)).append('|');
    sb.append(DatumFactory.createFloat8(271.9f)).append('|');
    sb.append(DatumFactory.createText("str2")).append('|');
    sb.append(DatumFactory.createBlob("jinho".getBytes())).append('|');
    sb.append(DatumFactory.createInet4("192.168.0.1")).append('|');
    sb.append(new String(nullbytes)).append('|');
    sb.append(NullDatum.get());
    textRow = BytesUtils.splitPreserveAllTokens(sb.toString().getBytes(), '|');
    serde = new TextSerializerDeserializer();
  }

  @Test
  public void testGetDatum() {

    LazyTuple t1 = new LazyTuple(schema, textRow, -1, nullbytes, serde);
    assertEquals(DatumFactory.createBool(true), t1.get(0));
    assertEquals(DatumFactory.createBit((byte) 0x99), t1.get(1));
    assertEquals(DatumFactory.createChar("str"), t1.get(2));
    assertEquals(DatumFactory.createInt2((short) 17), t1.get(3));
    assertEquals(DatumFactory.createInt4(59), t1.get(4));
    assertEquals(DatumFactory.createInt8(23l), t1.get(5));
    assertEquals(DatumFactory.createFloat4(77.9f), t1.get(6));
    assertEquals(DatumFactory.createFloat8(271.9f), t1.get(7));
    assertEquals(DatumFactory.createText("str2"), t1.get(8));
    assertEquals(DatumFactory.createBlob("jinho".getBytes()), t1.get(9));
    assertEquals(DatumFactory.createInet4("192.168.0.1"), t1.get(10));
    assertEquals(NullDatum.get(), t1.get(11));
    assertEquals(NullDatum.get(), t1.get(12));
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
    assertFalse(t1.contains(11));
    assertFalse(t1.contains(12));
  }

  @Test
  public void testPut() {
    int colNum = schema.size();
    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);
    t1.put(0, DatumFactory.createText("str"));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(11, DatumFactory.createFloat4(0.76f));

    assertTrue(t1.contains(0));
    assertTrue(t1.contains(1));

    assertEquals(t1.getText(0), "str");
    assertEquals(t1.get(1).asInt4(), 2);
    assertTrue(t1.get(11).asFloat4() == 0.76f);
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
  public void testPutTuple() {
    int colNum = schema.size();
    LazyTuple t1 = new LazyTuple(schema, new byte[colNum][], -1);

    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(2, DatumFactory.createInt4(3));


    Schema schema2 = new Schema();
    schema2.addColumn("col1", TajoDataTypes.Type.INT8);
    schema2.addColumn("col2", TajoDataTypes.Type.INT8);

    LazyTuple t2 = new LazyTuple(schema2, new byte[schema2.size()][], -1);
    t2.put(0, DatumFactory.createInt4(4));
    t2.put(1, DatumFactory.createInt4(5));

    t1.put(3, t2);

    for (int i = 0; i < 5; i++) {
      assertEquals(i + 1, t1.get(i).asInt4());
    }
  }

  @Test
  public void testInvalidNumber() {
    byte[][] bytes = BytesUtils.splitPreserveAllTokens(" 1| |2 ||".getBytes(), '|');
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);

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
