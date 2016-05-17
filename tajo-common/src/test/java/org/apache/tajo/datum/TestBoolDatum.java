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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestBoolDatum {

  @Test
  public final void testAsBool() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertEquals(true, trueDatum.asBool());
    Datum falseDatum = DatumFactory.createBool(false);
    assertEquals(false, falseDatum.asBool());
  }

  @Test
  public final void testAsShort() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertEquals(1, trueDatum.asInt2());
    Datum falseDatum = DatumFactory.createBool(false);
    assertEquals(2, falseDatum.asInt2());
  }

  @Test
  public final void testAsInt() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertEquals(1, trueDatum.asInt4());
    Datum falseDatum = DatumFactory.createBool(false);
    assertEquals(2, falseDatum.asInt4());
  }

  @Test
  public final void testAsLong() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertEquals(1, trueDatum.asInt8());
    Datum falseDatum = DatumFactory.createBool(false);
    assertEquals(2, falseDatum.asInt8());
  }

  @Test
  public final void testAsByte() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertEquals(0x01, trueDatum.asByte());
    Datum falseDatum = DatumFactory.createBool(false);
    assertEquals(0x02, falseDatum.asByte());
  }

  @Test
  public final void testAsChars() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertEquals("t", trueDatum.asChars());
    Datum falseDatum = DatumFactory.createBool(false);
    assertEquals("f", falseDatum.asChars());
  }

  @Test
  public final void testSize() {
    Datum d = DatumFactory.createBool(true);
    assertEquals(1, d.size());
  }

  @Test
  public final void testAsTextBytes() {
    Datum trueDatum = DatumFactory.createBool(true);
    assertArrayEquals(trueDatum.toString().getBytes(), trueDatum.asTextBytes());
    Datum falseDatum = DatumFactory.createBool(false);
    assertArrayEquals(falseDatum.toString().getBytes(), falseDatum.asTextBytes());
  }
}