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
import static org.junit.Assert.assertTrue;

public class TestDatum {

	@Test
	public final void testPlusDatumDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(1);
		y = DatumFactory.createInt(2);
		z = x.plus(y);
		assertEquals(z.type(), DatumType.INT);
		assertEquals(z.asInt(),3);		
		z = y.plus(x);
		assertEquals(z.type(),DatumType.INT);
		assertEquals(z.asInt(),3);
		
		x = DatumFactory.createInt(1);
		y = DatumFactory.createLong(2l);
		z = x.plus(y);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),3l);		
		z = y.plus(x);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),3l);
		
		y = DatumFactory.createFloat(2.5f);
		z = x.plus(y);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == 3.5f);
		z = y.plus(x);
		assertEquals(z.type(),DatumType.FLOAT);
		assertEquals(z.asInt(),3);
		
		y = DatumFactory.createDouble(4.5d);
		z = x.plus(y);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 5.5d);
		z = y.plus(x);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 5.5d);
	}

	@Test
	public final void testMinusDatumDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(5);
		y = DatumFactory.createInt(2);
		z = x.minus(y);
		assertEquals(z.type(),DatumType.INT);
		assertEquals(z.asInt(),3);		
		z = y.minus(x);
		assertEquals(z.type(),DatumType.INT);
		assertEquals(z.asInt(),-3);
		
		y = DatumFactory.createLong(2l);
		z = x.minus(y);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),3l);		
		z = y.minus(x);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),-3l);
		
		y = DatumFactory.createFloat(2.5f);
		z = x.minus(y);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == 2.5f);
		z = y.minus(x);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == -2.5f);
		
		y = DatumFactory.createDouble(4.5d);
		z = x.minus(y);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 0.5d);
		z = y.minus(x);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == -0.5d);
	}

	@Test
	public final void testMultiplyDatumDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(5);
		y = DatumFactory.createInt(2);
		z = x.multiply(y);
		assertEquals(z.type(),DatumType.INT);
		assertEquals(z.asInt(),10);		
		z = y.multiply(x);
		assertEquals(z.type(),DatumType.INT);
		assertEquals(z.asInt(),10);
		
		y = DatumFactory.createLong(2l);
		z = x.multiply(y);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),10l);		
		z = y.multiply(x);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),10l);
		
		y = DatumFactory.createFloat(2.5f);
		z = x.multiply(y);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == 12.5f);
		z = y.multiply(x);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == 12.5f);
		
		y = DatumFactory.createDouble(4.5d);
		z = x.multiply(y);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 22.5d);
		z = y.multiply(x);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 22.5d);
	}

	@Test
	public final void testDivideDatumDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(3);
		z = x.divide(y);
		assertEquals(z.type(),DatumType.INT);
		assertEquals(z.asInt(),2);		
		z = y.divide(x);
		assertEquals(z.type(),DatumType.INT);
		assertTrue(z.asInt() == 0);
		
		y = DatumFactory.createLong(3l);
		z = x.divide(y);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),2l);		
		z = y.divide(x);
		assertEquals(z.type(),DatumType.LONG);
		assertEquals(z.asLong(),0l);
		
		y = DatumFactory.createFloat(3f);
		z = x.divide(y);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == 2.0f);
		z = y.divide(x);
		assertEquals(z.type(),DatumType.FLOAT);
		assertTrue(z.asFloat() == 0.5f);
		
		y = DatumFactory.createDouble(3d);
		z = x.divide(y);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 2.0d);
		z = y.divide(x);
		assertEquals(z.type(),DatumType.DOUBLE);
		assertTrue(z.asDouble() == 0.5d);
	}
	
	@Test
	public final void testEqualToDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(3);
		z = x.equalsTo(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),false);		
		z = y.equalsTo(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), false);
		
		x = DatumFactory.createFloat(3.27f);
		y = DatumFactory.createFloat(3.27f);
		z = x.equalsTo(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),true);		
		z = y.equalsTo(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), true);
		
		x = DatumFactory.createLong(123456789012345l);
		y = DatumFactory.createLong(123456789012345l);
		z = x.equalsTo(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),true);		
		z = y.equalsTo(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), true);
	}
	
	@Test
	public final void testLessThanDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(3);
		z = x.lessThan(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),false);		
		z = y.lessThan(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), true);
	}
	
	@Test
	public final void testLessThanEqualsDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(3);
		z = x.lessThanEqual(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),false);		
		z = y.lessThanEqual(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), true);
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(6);
		z = x.lessThanEqual(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),true);		
		z = y.lessThanEqual(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), true);
	}
	
	@Test
	public final void testgreaterThanDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(3);
		z = x.greaterThan(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),true);		
		z = y.greaterThan(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), false);
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(6);
		z = x.greaterThan(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),false);		
		z = y.greaterThan(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), false);
	}
	
	@Test
	public final void testgreaterThanEqualsDatum() {
		Datum x;
		Datum y;
		Datum z = null;
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(3);
		z = x.greaterThanEqual(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),true);		
		z = y.greaterThanEqual(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), false);
		
		x = DatumFactory.createInt(6);
		y = DatumFactory.createInt(6);
		z = x.greaterThanEqual(y);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(),true);		
		z = y.greaterThanEqual(x);
		assertEquals(z.type(),DatumType.BOOLEAN);
		assertEquals(z.asBool(), true);
	}
}
