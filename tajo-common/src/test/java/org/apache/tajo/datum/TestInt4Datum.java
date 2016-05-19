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

package org.apache.tajo.datum;

import org.apache.tajo.common.TajoDataTypes;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestInt4Datum {

  @Test
  public final void testType() {
    Datum d = DatumFactory.createInt4(1);
    assertEquals(TajoDataTypes.Type.INT4, d.type());
  }

  @Test
  public final void testAsInt() {
    Datum d = DatumFactory.createInt4(5);
    assertEquals(5, d.asInt4());
  }

  @Test
  public final void testAsLong() {
    Datum d = DatumFactory.createInt4(5);
    assertEquals(5, d.asInt8());
  }

  @Test
  public final void testAsFloat() {
    Datum d = DatumFactory.createInt4(5);
    assertTrue(5.0f == d.asFloat4());
  }

  @Test
  public final void testAsDouble() {
    Datum d = DatumFactory.createInt4(5);
    assertTrue(5.0d == d.asFloat8());
  }

  @Test
  public final void testAsChars() {
    Datum d = DatumFactory.createInt4(5);
    assertEquals("5", d.asChars());
  }

  @Test
  public final void testSize() {
    Datum d = DatumFactory.createInt4(5);
    assertEquals(4, d.size());
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createInt4(5);
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }
}
