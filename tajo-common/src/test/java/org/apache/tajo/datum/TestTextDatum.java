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
import org.apache.tajo.common.TajoDataTypes.Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTextDatum {
	
	@Test
	public final void testType() {
		Datum d = DatumFactory.createText("12345");
		assertEquals(d.type(), Type.TEXT);
	}
	
	@Test
	public final void testAsInt4() {
		Datum d = DatumFactory.createText("12345");
		assertEquals(12345,d.asInt4());
	}

	@Test
	public final void testAsInt8() {
		Datum d = DatumFactory.createText("12345");
		assertEquals(12345l,d.asInt8());
	}

	@Test
	public final void testAsFloat4() {
		Datum d = DatumFactory.createText("12345");
		assertTrue(12345.0f == d.asFloat4());
	}

	@Test
	public final void testAsFloat8() {
		Datum d = DatumFactory.createText("12345");
		assertTrue(12345.0d == d.asFloat8());
	}

	@Test
	public final void testAsText() {
		Datum d = DatumFactory.createText("12345");
		assertEquals("12345", d.asChars());
	}
	
	@Test
  public final void testSize() {
	  Datum d = DatumFactory.createText("12345");
    assertEquals(5, d.size());
  }
}
