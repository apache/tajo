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
import static org.junit.Assert.assertTrue;

public class TestDatum {

	@Test
	public final void testPlusDatumDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(1);
		y = DatumFactory.createInt4(2);
		z = x.plus(y);
		assertEquals(z.type(), Type.Int4);
		assertEquals(z.asInt4(),3);
		z = y.plus(x);
		assertEquals(z.type(),Type.Int4);
		assertEquals(z.asInt4(),3);
		
		x = DatumFactory.createInt4(1);
		y = DatumFactory.createInt8(2l);
		z = x.plus(y);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),3l);
		z = y.plus(x);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),3l);
		
		y = DatumFactory.createFloat4(2.5f);
		z = x.plus(y);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == 3.5f);
		z = y.plus(x);
		assertEquals(z.type(),Type.Float4);
		assertEquals(z.asInt4(),3);
		
		y = DatumFactory.createFloat8(4.5d);
		z = x.plus(y);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 5.5d);
		z = y.plus(x);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 5.5d);
	}

	@Test
	public final void testMinusDatumDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(5);
		y = DatumFactory.createInt4(2);
		z = x.minus(y);
		assertEquals(z.type(),Type.Int4);
		assertEquals(z.asInt4(),3);
		z = y.minus(x);
		assertEquals(z.type(),Type.Int4);
		assertEquals(z.asInt4(),-3);
		
		y = DatumFactory.createInt8(2l);
		z = x.minus(y);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),3l);
		z = y.minus(x);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),-3l);
		
		y = DatumFactory.createFloat4(2.5f);
		z = x.minus(y);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == 2.5f);
		z = y.minus(x);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == -2.5f);
		
		y = DatumFactory.createFloat8(4.5d);
		z = x.minus(y);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 0.5d);
		z = y.minus(x);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == -0.5d);
	}

	@Test
	public final void testMultiplyDatumDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(5);
		y = DatumFactory.createInt4(2);
		z = x.multiply(y);
		assertEquals(z.type(),Type.Int4);
		assertEquals(z.asInt4(),10);
		z = y.multiply(x);
		assertEquals(z.type(),Type.Int4);
		assertEquals(z.asInt4(),10);
		
		y = DatumFactory.createInt8(2l);
		z = x.multiply(y);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),10l);
		z = y.multiply(x);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),10l);
		
		y = DatumFactory.createFloat4(2.5f);
		z = x.multiply(y);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == 12.5f);
		z = y.multiply(x);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == 12.5f);
		
		y = DatumFactory.createFloat8(4.5d);
		z = x.multiply(y);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 22.5d);
		z = y.multiply(x);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 22.5d);
	}

	@Test
	public final void testDivideDatumDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(3);
		z = x.divide(y);
		assertEquals(z.type(), Type.Int4);
		assertEquals(z.asInt4(),2);
		z = y.divide(x);
		assertEquals(z.type(),Type.Int4);
		assertTrue(z.asInt4() == 0);
		
		y = DatumFactory.createInt8(3l);
		z = x.divide(y);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),2l);
		z = y.divide(x);
		assertEquals(z.type(),Type.Int8);
		assertEquals(z.asInt8(),0l);
		
		y = DatumFactory.createFloat4(3f);
		z = x.divide(y);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == 2.0f);
		z = y.divide(x);
		assertEquals(z.type(),Type.Float4);
		assertTrue(z.asFloat4() == 0.5f);
		
		y = DatumFactory.createFloat8(3d);
		z = x.divide(y);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 2.0d);
		z = y.divide(x);
		assertEquals(z.type(),Type.Float8);
		assertTrue(z.asFloat8() == 0.5d);
	}
	
	@Test
	public final void testEqualToDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(3);
		z = x.equalsTo(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),false);		
		z = y.equalsTo(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), false);
		
		x = DatumFactory.createFloat4(3.27f);
		y = DatumFactory.createFloat4(3.27f);
		z = x.equalsTo(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),true);		
		z = y.equalsTo(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), true);
		
		x = DatumFactory.createInt8(123456789012345l);
		y = DatumFactory.createInt8(123456789012345l);
		z = x.equalsTo(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),true);		
		z = y.equalsTo(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), true);
	}
	
	@Test
	public final void testLessThanDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(3);
		z = x.lessThan(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),false);		
		z = y.lessThan(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), true);
	}
	
	@Test
	public final void testLessThanEqualsDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(3);
		z = x.lessThanEqual(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),false);		
		z = y.lessThanEqual(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), true);
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(6);
		z = x.lessThanEqual(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),true);		
		z = y.lessThanEqual(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), true);
	}
	
	@Test
	public final void testgreaterThanDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(3);
		z = x.greaterThan(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),true);		
		z = y.greaterThan(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), false);
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(6);
		z = x.greaterThan(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),false);		
		z = y.greaterThan(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), false);
	}
	
	@Test
	public final void testgreaterThanEqualsDatum() {
		Datum x;
		Datum y;
		Datum z;
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(3);
		z = x.greaterThanEqual(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),true);		
		z = y.greaterThanEqual(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), false);
		
		x = DatumFactory.createInt4(6);
		y = DatumFactory.createInt4(6);
		z = x.greaterThanEqual(y);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(),true);		
		z = y.greaterThanEqual(x);
		assertEquals(z.type(),Type.Bool);
		assertEquals(z.asBool(), true);
	}
}
