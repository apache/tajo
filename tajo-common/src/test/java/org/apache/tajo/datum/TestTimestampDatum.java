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

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.exception.InvalidCastException;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTimestampDatum {
	
	@Test
	public final void testType() {
    long instance = 1386577582;
		Datum d = DatumFactory.createTimeStamp(instance);
    assertEquals(Type.TIMESTAMP, d.type());
	}
	
	@Test(expected = InvalidCastException.class)
	public final void testAsInt4() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    d.asInt4();
	}

	@Test
	public final void testAsInt8() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    assertEquals(instance, d.asInt8());
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat4() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    d.asFloat4();
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat8() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    d.asFloat8();
	}

	@Test
	public final void testAsText() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    System.out.println(d.asChars());
	}

  @Test
  public final void testAsByteArray() {
    long instance = 1386577582;
    TimestampDatum d = DatumFactory.createTimeStamp(instance);
    TimestampDatum copy = new TimestampDatum(d.asByteArray());
    assertEquals(d.asInt8(), copy.asInt8());
  }
	
	@Test
  public final void testSize() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    assertEquals(TimestampDatum.SIZE, d.size());
  }

  @Test
  public final void testAsTextBytes() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }

  @Test
  public final void testToJson() {
    long instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    System.out.println(d.toJson());
  }
}
