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

import static org.apache.tajo.schema.Field.Field;
import static org.apache.tajo.schema.Field.Record;
import static org.apache.tajo.schema.Identifier._;
import static org.apache.tajo.schema.QualifiedIdentifier.$;
import static org.apache.tajo.schema.Schema.Schema;
import static org.apache.tajo.type.Type.*;
import static org.junit.Assert.assertEquals;

public class TestSchema {

  @Test
  public final void testSchema1() {
    Field struct1 = Record($("f12"), Field($("f1"), Int8), Field($("f2"), Int4));
    Field struct2 = Record($("f34"), Field($("f3"), Int8), Field($("f4"), Int4));
    Schema schema = Schema(struct1, struct2);

    assertEquals("f12 (RECORD(f1 (INT8), f2 (INT4))), f34 (RECORD(f3 (INT8), f4 (INT4)))", schema.toString());
  }

  @Test
  public final void testSchema2() {
    Field f1 = Field($("x"), Array(Int8));
    Field f2 = Field($("y"), Int8);
    Field f3 = Record($("z"), Field($("z-1"), Time), Field($("z-2"), Array(Int8)));
    Schema schema = Schema(f1, f2, f3);

    assertEquals("x (ARRAY<INT8>), y (INT8), z (RECORD(z-1 (TIME), z-2 (ARRAY<INT8>)))", schema.toString());
  }

  @Test
  public final void testSchemaWithIdentifiers() {
    Field f1 = Field($("x", "y"), Array(Int8));
    Field f2 = Field($(_("y"), _("B", true)), Int8);
    Field f3 = Record($("z"), Field($("z-1"), Time), Field($("z-2"), Array(Int8)));
    Schema schema = Schema(f1, f2, f3);

    assertEquals("x.y (ARRAY<INT8>), y.B (INT8), z (RECORD(z-1 (TIME), z-2 (ARRAY<INT8>)))", schema.toString());
  }
}