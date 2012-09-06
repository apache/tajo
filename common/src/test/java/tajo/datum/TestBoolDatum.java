/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBoolDatum {
	
	@Test
	public final void testType() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(DatumType.BOOLEAN, d.type());
	}
	
	@Test
	public final void testAsBool() {
		Datum d = DatumFactory.createBool(false);
		assertEquals(false, d.asBool());
	}
	
	@Test
	public final void testAsShort() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(1, d.asShort());
	}
	
	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(1, d.asInt());
	}
	
	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createBool(false);
		assertEquals(0, d.asLong());
	}
	
	@Test
	public final void testAsByte() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(0x01, d.asByte());
	}
	
	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createBool(true);
		assertEquals("true", d.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createBool(true);
    assertEquals(1, d.size());
  }
}