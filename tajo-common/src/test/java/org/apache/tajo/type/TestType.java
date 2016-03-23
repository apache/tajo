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
import static org.apache.tajo.type.Type.*;
import static org.junit.Assert.assertEquals;

public class TestType {
  @Test
  public final void testPrimitiveTypes() {
    assertEquals(Bool().baseType(), BOOLEAN);
    assertEquals(Int2().baseType(), INT2);
    assertEquals(Int4().baseType(), INT4);
    assertEquals(Int8().baseType(), INT8);
    assertEquals(Float4().baseType(), FLOAT4);
    assertEquals(Float8().baseType(), FLOAT8);
    assertEquals(Date().baseType(), DATE);
    assertEquals(Time().baseType(), TIME);
    assertEquals(Timestamp().baseType(), TIMESTAMP);

    Numeric n = Numeric(4, 2);
    assertEquals(n.baseType(), NUMERIC);
    assertEquals(n.precision(), 4);
    assertEquals(n.scale(), 2);

    assertEquals(Blob().baseType(), BLOB);

    Char c = Char(2);
    assertEquals(c.baseType(), CHAR);
    assertEquals(c.length(), 2);

    Varchar varchar = Varchar(2);
    assertEquals(varchar.baseType(), VARCHAR);
    assertEquals(varchar.length(), 2);

    Struct struct = Struct(Int8(), Array(Float8()));
    assertEquals(struct.baseType(), RECORD);
    assertEquals(struct.memberType(0).baseType(), INT8);
    assertEquals(struct.memberType(1).baseType(), ARRAY);

    Map map = Map(Int8(), Array(Timestamp()));
    assertEquals(map.baseType(), MAP);
    assertEquals(map.keyType().baseType(), INT8);
    assertEquals(map.valueType().baseType(), ARRAY);

    Array array = Array(Int8());
    assertEquals(array.baseType(), ARRAY);
    assertEquals(array.elementType().baseType(), INT8);
  }

  @Test
  public final void testToString() {
    assertEquals("boolean", Bool().toString());
    assertEquals("int2", Int2().toString());
    assertEquals("int4", Int4().toString());
    assertEquals("int8", Int8().toString());
    assertEquals("float4", Float4().toString());
    assertEquals("float8", Float8().toString());
    assertEquals("date", Date().toString());
    assertEquals("time", Time().toString());
    assertEquals("timestamp", Timestamp().toString());

    Numeric n = Numeric(4, 2);
    assertEquals("numeric(4,2)", n.toString());

    assertEquals("blob", Blob().toString());

    Char c = Char(2);
    assertEquals("char(2)", c.toString());

    Varchar varchar = Varchar(2);
    assertEquals("varchar(2)", varchar.toString());

    Struct struct = Struct(Int8(), Array(Float8()));
    assertEquals("struct(int8,array<float8>)", struct.toString());

    Map map = Map(Int8(), Array(Timestamp()));
    assertEquals("map<int8,array<timestamp>>", map.toString());

    Array array = Array(Int8());
    assertEquals("array<int8>", array.toString());
  }
}