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
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class TestTimestampDatum {
  private static long javatime;
  private static int unixtime;
  private static Calendar calendar;

  @BeforeClass
  public static void setUp() {
    javatime = System.currentTimeMillis();
    calendar = Calendar.getInstance(TimeZone.getTimeZone("PST"));
    calendar.setTimeInMillis(javatime);
    unixtime = (int) (javatime / 1000);
  }

	@Test
	public final void testType() {
		Datum d = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
    assertEquals(Type.TIMESTAMP, d.type());
	}

	@Test(expected = InvalidCastException.class)
	public final void testAsInt4() {
    Datum d = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
    d.asInt4();
	}

  @Test
	public final void testAsInt8() {
    Datum d = DatumFactory.createTimestmpDatumWithJavaMillis(unixtime * 1000);
    long javaTime = unixtime * 1000;
    assertEquals(DateTimeUtil.javaTimeToJulianTime(javaTime), d.asInt8());
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat4() {
    Datum d = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
    d.asFloat4();
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat8() {
    int instance = 1386577582;
    Datum d = DatumFactory.createTimestmpDatumWithUnixTime(instance);
    d.asFloat8();
	}

	@Test
	public final void testAsText() {
    Datum d = DatumFactory.createTimestamp("1980-04-01 01:50:01");
    Datum copy = DatumFactory.createTimestamp(d.asChars());
    assertEquals(d, copy);

    d = DatumFactory.createTimestamp("1980-04-01 01:50:01.10");
    copy = DatumFactory.createTimestamp(d.asChars());
    assertEquals(d, copy);
	}

  @Test
  public void testAsText2() {
    // TAJO-1366
    TimestampDatum datum = DatumFactory.createTimestamp("Mon Nov 03 00:03:00 +0000 2014");
    assertEquals("2014-11-03 00:03:00", datum.asChars());
  }

	@Test
  public final void testSize() {
    Datum d = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
    assertEquals(TimestampDatum.SIZE, d.asByteArray().length);
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createTimestamp("1980-04-01 01:50:01");
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());

    d = DatumFactory.createTimestamp("1980-04-01 01:50:01.578");
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }

  @Test
  public final void testToJson() {
    Datum d = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
    Datum copy = CommonGsonHelper.fromJson(d.toJson(), Datum.class);
    assertEquals(d, copy);
  }

  @Test
  public final void testTimeZone() {
    TimestampDatum datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(2014, 5, 1, 15, 20, 30, 0));
    assertEquals("2014-05-01 15:20:30", datum.asChars());
    assertEquals("2014-05-02 00:20:30+09", TimestampDatum.asChars(datum.asTimeMeta(), TimeZone.getTimeZone("GMT+9"), true));
  }

  @Test
  public final void testTimestampConstructor() {
    TimestampDatum datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(2014, 5, 1, 10, 20, 30, 0));
    assertEquals(2014, datum.getYear());
    assertEquals(5, datum.getMonthOfYear());
    assertEquals(1, datum.getDayOfMonth());
    assertEquals(10, datum.getHourOfDay());
    assertEquals(20, datum.getMinuteOfHour());
    assertEquals(30, datum.getSecondOfMinute());

    TimestampDatum datum2 = DatumFactory.createTimestamp("2014-05-01 10:20:30");
    assertEquals(datum2, datum);

    datum = DatumFactory.createTimestamp("1980-04-01 01:50:01.123");
    assertEquals(1980, datum.getYear());
    assertEquals(4, datum.getMonthOfYear());
    assertEquals(1, datum.getDayOfMonth());
    assertEquals(1, datum.getHourOfDay());
    assertEquals(50, datum.getMinuteOfHour());
    assertEquals(1, datum.getSecondOfMinute());
    assertEquals(123, datum.getMillisOfSecond());

    datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(1014, 5, 1, 10, 20, 30, 0));
    assertEquals(1014, datum.getYear());
    assertEquals(5, datum.getMonthOfYear());
    assertEquals(1, datum.getDayOfMonth());
    assertEquals(10, datum.getHourOfDay());
    assertEquals(20, datum.getMinuteOfHour());
    assertEquals(30, datum.getSecondOfMinute());

    datum2 = DatumFactory.createTimestamp("1014-05-01 10:20:30");
    assertEquals(datum2, datum);

    for (int i = 0; i < 100; i++) {
      TimeZone timeZone = TimeZone.getTimeZone("GMT");
      Calendar cal = Calendar.getInstance(timeZone);
      long jTime = System.currentTimeMillis();
      int uTime = (int)(jTime / 1000);
      cal.setTimeInMillis(jTime);

      long julianTimestamp = DateTimeUtil.javaTimeToJulianTime(jTime);
      assertEquals(uTime, DateTimeUtil.julianTimeToEpoch(julianTimestamp));
      assertEquals(jTime, DateTimeUtil.julianTimeToJavaTime(julianTimestamp));

      TimestampDatum datum3 = DatumFactory.createTimestmpDatumWithJavaMillis(jTime);
      assertEquals(cal.get(Calendar.YEAR), datum3.getYear());
      assertEquals(cal.get(Calendar.MONTH) + 1, datum3.getMonthOfYear());
      assertEquals(cal.get(Calendar.DAY_OF_MONTH), datum3.getDayOfMonth());

      datum3 = DatumFactory.createTimestmpDatumWithUnixTime(uTime);
      assertEquals(cal.get(Calendar.YEAR), datum3.getYear());
      assertEquals(cal.get(Calendar.MONTH) + 1, datum3.getMonthOfYear());
      assertEquals(cal.get(Calendar.DAY_OF_MONTH), datum3.getDayOfMonth());
    }
  }

  @Test
  public final void testNull() {
   Datum d = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
   assertEquals(Boolean.FALSE,d.equals(DatumFactory.createNullDatum()));
   assertEquals(DatumFactory.createNullDatum(),d.equalsTo(DatumFactory.createNullDatum()));
   assertEquals(-1,d.compareTo(DatumFactory.createNullDatum()));
  }
  
  @Test
  public void testCompareTo() {
    TimestampDatum theday = DatumFactory.createTimestamp("2014-11-12 15:00:00.68");
    TimestampDatum thedaybefore = DatumFactory.createTimestamp("2014-11-11 15:00:00.56");
    
    assertThat(theday.compareTo(thedaybefore) > 0, is(true));
    assertThat(thedaybefore.compareTo(theday) > 0, is(false));
    
    DateDatum date = DatumFactory.createDate("2014-11-12");
    
    assertThat(theday.compareTo(date) > 0, is(true));
  }
}
