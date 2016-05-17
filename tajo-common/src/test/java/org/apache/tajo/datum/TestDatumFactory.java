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

import static org.junit.Assert.assertEquals;

public class TestDatumFactory {
	
	@Test
	public final void testCreateBit() {
		Datum d = DatumFactory.createBit((byte) 5);
		assertEquals(Type.Bit, d.type());
	}

	@Test
	public final void testCreateInt2() {
		Datum d = DatumFactory.createInt2((short) 5);
		assertEquals(Type.Int2, d.type());
	}
	
	@Test
	public final void testCreateInt4() {
		Datum d = DatumFactory.createInt4(5);
		assertEquals(Type.Int4, d.type());
	}
	
	@Test
	public final void testCreateInt8() {
		Datum d = DatumFactory.createInt8((long) 5);
		assertEquals(Type.Int8, d.type());
	}

	@Test
	public final void testCreateFloat4() {
		Datum d = DatumFactory.createFloat4(5.0f);
		assertEquals(Type.Float4, d.type());
	}

	@Test
	public final void testCreateFloat8() {
		Datum d = DatumFactory.createFloat8(5.0d);
		assertEquals(Type.Float8, d.type());
	}

	@Test
	public final void testCreateBoolean() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(Type.Bool, d.type());
	}

	@Test
	public final void testCreateString() {
		Datum d = DatumFactory.createText("12345a");
		assertEquals(Type.Text, d.type());
	}
}
