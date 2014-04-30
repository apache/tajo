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
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.json.CommonGsonHelper;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTimestampDatum {

  private static int timestamp;

  @BeforeClass
  public static void setUp() {
    timestamp = (int) (System.currentTimeMillis() / 1000);
  }

	@Test
	public final void testType() {
		Datum d = DatumFactory.createTimeStamp(timestamp);
        assertEquals(Type.TIMESTAMP, d.type());
	}

	@Test(expected = InvalidCastException.class)
	public final void testAsInt4() {
    Datum d = DatumFactory.createTimeStamp(timestamp);
    d.asInt4();
	}

  @Test
	public final void testAsInt8() {
    Datum d = DatumFactory.createTimeStamp(timestamp);
    long javaTime = timestamp * 1000l;
    assertEquals(javaTime, d.asInt8());
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat4() {
    Datum d = DatumFactory.createTimeStamp(timestamp);
    d.asFloat4();
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat8() {
    int instance = 1386577582;
    Datum d = DatumFactory.createTimeStamp(instance);
    d.asFloat8();
	}

	@Test
	public final void testAsText() {
    Datum d = DatumFactory.createTimeStamp("1980-04-01 01:50:01");
    Datum copy = DatumFactory.createTimeStamp(d.asChars());
    assertEquals(d, copy);
	}

  @Test
  public final void testAsByteArray() {
    TimestampDatum d = DatumFactory.createTimeStamp(timestamp);
    TimestampDatum copy = new TimestampDatum(d.asByteArray());
    assertEquals(d, copy);
  }

	@Test
  public final void testSize() {
    Datum d = DatumFactory.createTimeStamp(timestamp);
    assertEquals(TimestampDatum.SIZE, d.asByteArray().length);
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createTimeStamp("1980-04-01 01:50:01");
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());

    d = DatumFactory.createTimeStamp("1980-04-01 01:50:01.578");
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }

  @Test
  public final void testToJson() {
    Datum d = DatumFactory.createTimeStamp(timestamp);
    Datum copy = CommonGsonHelper.fromJson(d.toJson(), Datum.class);
    assertEquals(d, copy);
  }

  @Test
  public final void testGetFields() {
    TimestampDatum d = DatumFactory.createTimeStamp("1980-04-01 01:50:01");
    assertEquals(1980, d.getYear());
    assertEquals(4, d.getMonthOfYear());
    assertEquals(1, d.getDayOfMonth());
    assertEquals(1, d.getHourOfDay());
    assertEquals(50, d.getMinuteOfHour());
    assertEquals(01, d.getSecondOfMinute());
  }

  @Test
  public final void testNull() {
   Datum d = DatumFactory.createTimeStamp(timestamp);
   assertEquals(Boolean.FALSE,d.equals(DatumFactory.createNullDatum()));
   assertEquals(DatumFactory.createNullDatum(),d.equalsTo(DatumFactory.createNullDatum()));
   assertEquals(-1,d.compareTo(DatumFactory.createNullDatum()));
  }
}
