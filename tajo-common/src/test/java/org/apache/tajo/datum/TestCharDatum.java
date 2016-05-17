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

import org.apache.tajo.type.Type;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCharDatum {

	@Test
	public final void testType() {
		Datum d = DatumFactory.createChar((char) 1);
		assertEquals(Type.Char(1), d.type());
	}

	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createChar("5");
		assertEquals(5,d.asInt4());
	}
	
	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createChar("5");
		assertEquals(5l,d.asInt8());
	}
	
	@Test
	public final void testAsByte() {
		Datum d = DatumFactory.createChar((char)5);
		assertEquals(5,d.asByte());
	}

	@Test
	public final void testAsFloat() {
		Datum d = DatumFactory.createChar("5");
		assertTrue(5.0f == d.asFloat4());
	}

	@Test
	public final void testAsDouble() {
		Datum d = DatumFactory.createChar("5");
		assertTrue(5.0d == d.asFloat8());
	}
	
	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createChar("1234567890");
    assertEquals("1234567890", d.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createChar("1234567890".getBytes());
    assertEquals(10, d.size());
    d = DatumFactory.createChar("1234567890");
    assertEquals(10, d.size());
  }

  @Test
  public final void testCompare() {
    Datum a = DatumFactory.createChar("abc");
    Datum b = DatumFactory.createChar("abd");
    Datum c = DatumFactory.createChar("ace");

    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertTrue(c.compareTo(c) == 0);
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createChar("1234567890");
    assertArrayEquals(d.asByteArray(), d.asTextBytes());
  }
}