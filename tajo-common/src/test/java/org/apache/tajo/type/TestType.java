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

package org.apache.tajo.type;

import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.schema.Field.Field;
import static org.apache.tajo.schema.QualifiedIdentifier.$;
import static org.apache.tajo.type.Type.Array;
import static org.apache.tajo.type.Type.Blob;
import static org.apache.tajo.type.Type.Bool;
import static org.apache.tajo.type.Type.Char;
import static org.apache.tajo.type.Type.Date;
import static org.apache.tajo.type.Type.Float4;
import static org.apache.tajo.type.Type.Float8;
import static org.apache.tajo.type.Type.Int1;
import static org.apache.tajo.type.Type.Int2;
import static org.apache.tajo.type.Type.Int4;
import static org.apache.tajo.type.Type.Int8;
import static org.apache.tajo.type.Type.Map;
import static org.apache.tajo.type.Type.Numeric;
import static org.apache.tajo.type.Type.Time;
import static org.apache.tajo.type.Type.Timestamp;
import static org.apache.tajo.type.Type.Varchar;
import static org.junit.Assert.assertEquals;

public class TestType {
  @Test
  public final void testPrimitiveTypes() {
    assertEquals(Bool.kind(), BOOLEAN);
    assertEquals(Int1.kind(), INT1);
    assertEquals(Int2.kind(), INT2);
    assertEquals(Int4.kind(), INT4);
    assertEquals(Int8.kind(), INT8);
    assertEquals(Float4.kind(), FLOAT4);
    assertEquals(Float8.kind(), FLOAT8);
    assertEquals(Date.kind(), DATE);
    assertEquals(Time.kind(), TIME);
    assertEquals(Timestamp.kind(), TIMESTAMP);

    Numeric n = Numeric(4, 2);
    assertEquals(n.kind(), NUMERIC);
    assertEquals(n.precision(), 4);
    assertEquals(n.scale(), 2);

    assertEquals(Blob.kind(), BLOB);

    Char c = Char(2);
    assertEquals(c.kind(), CHAR);
    assertEquals(c.length(), 2);

    Varchar varchar = Varchar(2);
    assertEquals(varchar.kind(), VARCHAR);
    assertEquals(varchar.length(), 2);

    Record record = Type.Record(Field("x", Int8), Field("y", Array(Float8)));
    assertEquals(record.kind(), RECORD);
    assertEquals(record.field(0).baseType(), INT8);
    assertEquals(record.field(0).name(), $("x"));
    assertEquals(record.field(1).baseType(), ARRAY);
    assertEquals(record.field(1).name(), $("y"));

    Map map = Map(Int8, Array(Timestamp));
    assertEquals(map.kind(), MAP);
    assertEquals(map.keyType().kind(), INT8);
    assertEquals(map.valueType().kind(), ARRAY);

    Array array = Array(Int8);
    assertEquals(array.kind(), ARRAY);
    assertEquals(array.elementType().kind(), INT8);
  }

  @Test
  public final void testToString() {
    assertEquals("BOOLEAN", Bool.toString());
    assertEquals("INT1", Int1.toString());
    assertEquals("INT2", Int2.toString());
    assertEquals("INT4", Int4.toString());
    assertEquals("INT8", Int8.toString());
    assertEquals("FLOAT4", Float4.toString());
    assertEquals("FLOAT8", Float8.toString());
    assertEquals("DATE", Date.toString());
    assertEquals("TIME", Time.toString());
    assertEquals("TIMESTAMP", Timestamp.toString());

    Numeric n = Numeric(4, 2);
    assertEquals("NUMERIC(4,2)", n.toString());

    assertEquals("BLOB", Blob.toString());

    Char c = Char(2);
    assertEquals("CHAR(2)", c.toString());

    Varchar varchar = Varchar(2);
    assertEquals("VARCHAR(2)", varchar.toString());

    Record record = Type.Record(Field("x", Int8), Field("y", Array(Float8)));
    assertEquals("RECORD(x (INT8), y (ARRAY<FLOAT8>))", record.toString());

    Map map = Map(Int8, Array(Timestamp));
    assertEquals("MAP<INT8,ARRAY<TIMESTAMP>>", map.toString());

    Array array = Array(Int8);
    assertEquals("ARRAY<INT8>", array.toString());
  }
}