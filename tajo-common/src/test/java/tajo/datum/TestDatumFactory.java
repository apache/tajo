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

package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDatumFactory {
	
	@Test
	public final void testCreateByte() {
		Datum d = DatumFactory.createByte((byte) 5);
		assertEquals(DatumType.BYTE, d.type());
	}

	@Test
	public final void testCreateShort() {
		Datum d = DatumFactory.createShort((short)5);
		assertEquals(DatumType.SHORT, d.type());
	}
	
	@Test
	public final void testCreateInt() {
		Datum d = DatumFactory.createInt(5);
		assertEquals(DatumType.INT, d.type());
	}
	
	@Test
	public final void testCreateLong() {
		Datum d = DatumFactory.createLong((long)5);
		assertEquals(DatumType.LONG, d.type());
	}

	@Test
	public final void testCreateFloat() {
		Datum d = DatumFactory.createFloat(5.0f);
		assertEquals(DatumType.FLOAT, d.type());
	}

	@Test
	public final void testCreateDouble() {
		Datum d = DatumFactory.createDouble(5.0d);
		assertEquals(DatumType.DOUBLE, d.type());
	}

	@Test
	public final void testCreateBoolean() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(DatumType.BOOLEAN, d.type());
	}

	@Test
	public final void testCreateString() {
		Datum d = DatumFactory.createString("12345a");
		assertEquals(DatumType.STRING, d.type());
	}
}
