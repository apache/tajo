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

import com.google.common.base.Function;
import org.apache.tajo.schema.Field;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.tajo.schema.Field.Field;
import static org.apache.tajo.schema.Identifier._;
import static org.apache.tajo.schema.QualifiedIdentifier.$;
import static org.apache.tajo.type.Type.Array;
import static org.apache.tajo.type.Type.Blob;
import static org.apache.tajo.type.Type.Bool;
import static org.apache.tajo.type.Type.Char;
import static org.apache.tajo.type.Type.Float4;
import static org.apache.tajo.type.Type.Float8;
import static org.apache.tajo.type.Type.Int1;
import static org.apache.tajo.type.Type.Int2;
import static org.apache.tajo.type.Type.Int4;
import static org.apache.tajo.type.Type.Int8;
import static org.apache.tajo.type.Type.Map;
import static org.apache.tajo.type.Type.Numeric;
import static org.apache.tajo.type.Type.Record;
import static org.apache.tajo.type.Type.Text;
import static org.apache.tajo.type.Type.Varchar;
import static org.apache.tajo.type.TypeStringEncoder.*;
import static org.junit.Assert.assertEquals;

public abstract class TestTypeEncoder {

  public static final Record RECORD_POINT = Record(Field($("x"), Float8), Field($("y"), Float8));
  public static final Record RECORD_PERSON =
      Record(
          Field.Record($("name"), Field($(_("FirstName", true)), Text), Field($(_("LastName", true)), Text)),
          Field($("age"), Int2),
          Field.Record($("addr"), Field($("city"), Text), Field($("state"), Text))
      );

  @Test
  public final void testTypesWithoutParams() {
    assertSerialize(Bool);
    assertSerialize(Int1);
    assertSerialize(Int2);
    assertSerialize(Int4);
    assertSerialize(Int8);
    assertSerialize(Float4);
    assertSerialize(Float8);
    assertSerialize(Text);
    assertSerialize(Blob);
  }

  @Test
  public final void testValueParams() {
    //assertSerialize(Numeric());
    assertSerialize(Numeric(10));
    assertSerialize(Numeric(10, 12));

    assertSerialize(Char(256));
    assertSerialize(Varchar(256));
  }

  @Test
  public final void testTypeParams() {
    assertSerialize(Array(Float8));
    assertSerialize(Map(Text, Float8));

    // nested
    assertSerialize(Array(Array(Array(Float8))));
    assertSerialize(Array(Map(Text, Array(Float8))));
    assertSerialize(Map(Text, Array(Float8)));
    assertSerialize(Map(Text, Array(RECORD_POINT)));
  }

  @Test
  public final void testFieldParams() {
    assertSerialize(RECORD_POINT);
    assertSerialize(Record(Field.Record($("rectangle"),
        Field($("left-bottom"), RECORD_POINT),
        Field($("right-top"), RECORD_POINT))));

    // nested with quoted identifiers
    assertSerialize(RECORD_PERSON);
    assertSerialize(Record(Field.Record($("couple"),
        Field($("husband"), RECORD_PERSON),
        Field($("wife"), RECORD_PERSON))));
  }

  @Test
  public final void testParseTypeList() {
    List<Type> types = parseList("TEXT,ARRAY<FLOAT8>", new Function<String, Type>() {
      @Override
      public Type apply(@Nullable String s) {
        return decode(s);
      }
    });
    assertEquals(2, types.size());
    assertEquals(Text, types.get(0));
    assertEquals(Array(Float8), types.get(1));
  }

  abstract void assertSerialize(Type type);
}