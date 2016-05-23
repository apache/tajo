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
import org.apache.tajo.util.NumberUtil;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBitDatum {

	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createBit((byte) 5);
		assertEquals(5,d.asInt4());
	}
	
	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createBit((byte) 5);
		assertEquals(5l,d.asInt8());
	}
	
	@Test
	public final void testAsByte() {
		Datum d = DatumFactory.createBit((byte) 5);
		assertEquals(5,d.asInt8());
	}

	@Test
	public final void testAsFloat() {
		Datum d = DatumFactory.createBit((byte) 5);
		assertTrue(5.0f == d.asFloat4());
	}

	@Test
	public final void testAsDouble() {
		Datum d = DatumFactory.createBit((byte) 5);
		assertTrue(5.0d == d.asFloat8());
	}
	
	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createBit((byte) 'a');
		assertEquals("0x97", d.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createBit((byte) 1);
    assertEquals(1, d.size());
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createBit((byte) 5);
    assertArrayEquals(NumberUtil.toAsciiBytes(d.asInt4()), d.asTextBytes());
  }
}
