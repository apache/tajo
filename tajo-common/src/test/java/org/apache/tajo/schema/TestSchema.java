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

package org.apache.tajo.schema;

import org.junit.Test;

import static org.apache.tajo.schema.Schema.*;
import static org.apache.tajo.type.Type.*;
import static org.junit.Assert.assertEquals;

public class TestSchema {

  @Test
  public final void testSchema1() {
    NamedType struct1 = Struct("f12", Field("f1", Int8()), Field("f2", Int4()));
    NamedType struct2 = Struct("f34", Field("f3", Int8()), Field("f4", Int4()));
    Schema schema = Schema(struct1, struct2);

    assertEquals(schema.toString(), "f12 record (f1 int8,f2 int4),f34 record (f3 int8,f4 int4)");
  }

  @Test
  public final void testSchema2() {
    NamedType f1 = Field("x", Array(Int8()));
    NamedType f2 = Field("y", Int8());
    NamedType f3 = Struct("z", Field("z-1", Time()), Field("z-2", Array(Int8())));
    Schema schema = Schema(f1, f2, f3);

    assertEquals(schema.toString(), "x array<int8>,y int8,z record (z-1 time,z-2 array<int8>)");
  }
}