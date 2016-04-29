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

package org.apache.tajo.util;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.util.datetime.DateTimeConstants;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class TestDateTimeUtil {
  private static final TimeMeta TEST_DATETIME = DateTimeUtil.decodeDateTime("2014-04-18 01:15:25.69148");

  @Test
  public void testDecodeDateTime() {
    // http://www.postgresql.org/docs/9.1/static/datatype-datetime.html
    TimeMeta tm = DateTimeUtil.decodeDateTime("2014-01-07 14:12:54+09");
    assertEquals(2014, tm.years);
    assertEquals(1, tm.monthOfYear);
    assertEquals(7, tm.dayOfMonth);
    assertEquals(14, tm.hours);
    assertEquals(12, tm.minutes);
    assertEquals(54, tm.secs);
    assertEquals(0, tm.fsecs);

    tm = DateTimeUtil.decodeDateTime("1999-01-08 04:05:06.789");
    assertEquals(1999, tm.years);
    assertEquals(1, tm.monthOfYear);
    assertEquals(8, tm.dayOfMonth);
    assertEquals(4, tm.hours);
    assertEquals(5, tm.minutes);
    assertEquals(6, tm.secs);
    assertEquals(7 * 100000 + 8 * 10000 + 9 * 1000, tm.fsecs);

    TimeMeta tm2 = DateTimeUtil.decodeDateTime("January 8, 1999 04:05:06.789");
    assertEquals(tm, tm2);

    try {
      tm2 = DateTimeUtil.decodeDateTime("January 8, 99 04:05:06.789");
      assertEquals(tm, tm2);
      fail("error in YMD mode");
    } catch (Exception e) {
      //throws Exception in YMD mode
      //BAD Format: day overflow:99
    }

    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_MDY);
    tm2 = DateTimeUtil.decodeDateTime("January 8, 99 04:05:06.789");
    assertEquals(tm, tm2);

    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_YMD);
    tm2 = DateTimeUtil.decodeDateTime("1999/1/8 04:05:06.789");
    assertEquals(tm, tm2);

    tm2 = DateTimeUtil.decodeDateTime("1999/01/08 04:05:06.789");
    assertEquals(tm, tm2);

    //January 2, 2003 in MDY mode; February 1, 2003 in DMY mode; February 3, 2001 in YMD mode
    tm2 = DateTimeUtil.decodeDateTime("01/02/03 04:05:06.789");
    assertEquals(2001, tm2.years);
    assertEquals(2, tm2.monthOfYear);
    assertEquals(3, tm2.dayOfMonth);
    assertEquals(4, tm2.hours);
    assertEquals(5, tm2.minutes);
    assertEquals(6, tm2.secs);
    assertEquals(7 * 100000 + 8 * 10000 + 9 * 1000, tm2.fsecs);

    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_MDY);
    tm2 = DateTimeUtil.decodeDateTime("01/02/03 04:05:06.789");
    assertEquals(2003, tm2.years);
    assertEquals(1, tm2.monthOfYear);
    assertEquals(2, tm2.dayOfMonth);
    assertEquals(4, tm2.hours);
    assertEquals(5, tm2.minutes);
    assertEquals(6, tm2.secs);
    assertEquals(7 * 100000 + 8 * 10000 + 9 * 1000, tm2.fsecs);

    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_DMY);
    tm2 = DateTimeUtil.decodeDateTime("01/02/03 04:05:06.789");
    assertEquals(2003, tm2.years);
    assertEquals(2, tm2.monthOfYear);
    assertEquals(1, tm2.dayOfMonth);
    assertEquals(4, tm2.hours);
    assertEquals(5, tm2.minutes);
    assertEquals(6, tm2.secs);
    assertEquals(7 * 100000 + 8 * 10000 + 9 * 1000, tm2.fsecs);

    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_YMD);
    tm2 = DateTimeUtil.decodeDateTime("1999-Jan-08 04:05:06.789");
    assertEquals(tm, tm2);

    tm2 = DateTimeUtil.decodeDateTime("Jan-08-1999 04:05:06.789");
    assertEquals(tm, tm2);

    tm2 = DateTimeUtil.decodeDateTime("08-Jan-1999 04:05:06.789");
    assertEquals(tm, tm2);

    tm2 = DateTimeUtil.decodeDateTime("99-Jan-08 04:05:06.789");
    assertEquals(tm, tm2);

    //January 8, except error in YMD mode
    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_MDY);
    tm2 = DateTimeUtil.decodeDateTime("08-Jan-99 04:05:06.789");
    assertEquals(tm, tm2);

    //January 8, except error in YMD mode
    tm2 = DateTimeUtil.decodeDateTime("Jan-08-99 04:05:06.789");
    assertEquals(tm, tm2);

    TajoConf.setDateOrder(DateTimeConstants.DATEORDER_YMD);
    tm2 = DateTimeUtil.decodeDateTime("19990108 04:05:06.789");
    assertEquals(tm, tm2);

    tm2 = DateTimeUtil.decodeDateTime("990108 04:05:06.789");
    assertEquals(tm, tm2);

    //year and day of year
    tm2 = DateTimeUtil.decodeDateTime("1999.008");
    assertEquals(1999, tm2.years);
    assertEquals(1, tm2.monthOfYear);
    assertEquals(8, tm2.dayOfMonth);

    //BC
    tm = DateTimeUtil.decodeDateTime("19990108 BC 04:05:06.789");
    assertEquals(-1998, tm.years);
    assertEquals(1, tm.monthOfYear);
    assertEquals(8, tm.dayOfMonth);
    assertEquals(4, tm.hours);
    assertEquals(5, tm.minutes);
    assertEquals(6, tm.secs);
    assertEquals(7 * 100000 + 8 * 10000 + 9 * 1000, tm.fsecs);

    //PM
    tm = DateTimeUtil.decodeDateTime("2013-04-25 10:20:30.4 PM");
    assertEquals(2013, tm.years);
    assertEquals(4, tm.monthOfYear);
    assertEquals(25, tm.dayOfMonth);
    assertEquals(22, tm.hours);
    assertEquals(20, tm.minutes);
    assertEquals(30, tm.secs);
    assertEquals(4 * 100000, tm.fsecs);

    // date only
    tm = DateTimeUtil.decodeDateTime("1980-04-01");
    assertEquals(1980, tm.years);
    assertEquals(4, tm.monthOfYear);
    assertEquals(1, tm.dayOfMonth);
  }

  @Test
  public void testToJulianTimestamp() {
    long julian = DateTimeUtil.toJulianTimestamp("2013-04-25");
    assertEquals(julian, DateTimeUtil.toJulianTimestamp("2013-4-25"));
    assertEquals(julian, DateTimeUtil.toJulianTimestamp("2013.4.25"));
  }

  @Test
  public void testTimestampToJavaOrUnix() {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    long javaTime = System.currentTimeMillis();
    cal.setTimeInMillis(javaTime);

    long julianTimestamp = DateTimeUtil.javaTimeToJulianTime(cal.getTimeInMillis());

    assertEquals(javaTime, DateTimeUtil.julianTimeToJavaTime(julianTimestamp));
    assertEquals(javaTime/1000, DateTimeUtil.julianTimeToEpoch(julianTimestamp));
  }

  @Test
  public void testLeapYear() {
    assertTrue(DateTimeUtil.isLeapYear(2000));
    assertTrue(DateTimeUtil.isLeapYear(2004));
    assertTrue(DateTimeUtil.isLeapYear(1600));
    assertFalse(DateTimeUtil.isLeapYear(1900));
    assertFalse(DateTimeUtil.isLeapYear(2005));
  }

  @Test
  public void testAddMonthsToTimeMeta() {
    // Leap year
    String dateTimeStr = "2000-01-29 23:11:50.123";
    TimeMeta tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusMonths(1);
    assertEquals("2000-02-29 23:11:50.123", tm.toString());

    // Non leap year
    dateTimeStr = "1999-01-29 23:11:50.123";
    tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusMonths(1);
    assertEquals("1999-02-28 23:11:50.123", tm.toString());

    // changing year
    dateTimeStr = "2013-09-30 23:11:50.123";
    tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusMonths(5);
    assertEquals("2014-02-28 23:11:50.123", tm.toString());

    // minus value
    dateTimeStr = "2013-03-30 23:11:50.123";
    tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusMonths(-5);
    assertEquals("2012-10-30 23:11:50.123", tm.toString());
  }

  @Test
  public void testAddDaysToTimeMeta() {
    // Leap year
    String dateTimeStr = "2000-02-29 23:11:50.123";
    TimeMeta tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusDays(1);
    assertEquals("2000-03-01 23:11:50.123", tm.toString());

    // Non leap year
    dateTimeStr = "1999-01-29 23:11:50.123";
    tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusDays(1);
    assertEquals("1999-01-30 23:11:50.123", tm.toString());

    // changing year
    dateTimeStr = "2013-12-25 23:11:50.123";
    tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusDays(7);
    assertEquals("2014-01-01 23:11:50.123", tm.toString());

    // minus value
    dateTimeStr = "2000-03-05 23:11:50.123";
    tm = DateTimeUtil.decodeDateTime(dateTimeStr);
    tm.plusDays(-10);
    assertEquals("2000-02-24 23:11:50.123", tm.toString());
  }

  @Test
  public void testEncodeDateTime() throws Exception {
    //DateTimeUtil.encodeDateTime()

  }

  @Test
  public void testAppendSeconds() throws Exception {
    String[] fractions = new String[]{".999999", ".99999", ".9999", ".999", ".99", ".9", ""};

    for (int i = 0; i < fractions.length; i++) {
      StringBuilder sb = new StringBuilder("13:52:");
      DateTimeUtil.appendSecondsToEncodeOutput(sb, 23, 999999, 6 - i, false);
      assertEquals("13:52:23" + fractions[i], sb.toString());
    }

    fractions = new String[]{".1", ".01", ".001", ".0001", ".00001", ".000001"};
    for (int i = 0; i < fractions.length; i++) {
      StringBuilder sb = new StringBuilder("13:52:");
      DateTimeUtil.appendSecondsToEncodeOutput(sb, 23, (int)Math.pow(10, (5 - i)), 6, false);
      assertEquals("13:52:23" + fractions[i], sb.toString());
    }
  }

  @Test
  public void testTrimTrailingZeros() throws Exception {
    StringBuilder sb1 = new StringBuilder("1.1200");
    DateTimeUtil.trimTrailingZeros(sb1);
    assertEquals("1.12", sb1.toString());

    StringBuilder sb2 = new StringBuilder("1.12000120");
    DateTimeUtil.trimTrailingZeros(sb2);
    assertEquals("1.1200012", sb2.toString());

    StringBuilder sb3 = new StringBuilder(".12000120");
    DateTimeUtil.trimTrailingZeros(sb3);
    assertEquals(".1200012", sb3.toString());
  }

  @Test
  public void testTimeMeta() {
    TimeMeta tm = DateTimeUtil.decodeDateTime("2014-12-31");
    assertEquals(365, tm.getDayOfYear());

    tm = DateTimeUtil.decodeDateTime("2000-03-01");
    assertEquals(61, tm.getDayOfYear());

    tm = DateTimeUtil.decodeDateTime("2014-01-01");
    assertEquals(3, tm.getDayOfWeek());
    assertEquals(1, tm.getWeekOfYear());
    assertEquals(21, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("2000-03-01");
    assertEquals(3, tm.getDayOfWeek());
    assertEquals(9, tm.getWeekOfYear());
    assertEquals(20, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("1752-09-14");
    assertEquals(4, tm.getDayOfWeek());
    assertEquals(37, tm.getWeekOfYear());
    assertEquals(18, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("1752-09-02");
    assertEquals(6, tm.getDayOfWeek());
    assertEquals(35, tm.getWeekOfYear());
    assertEquals(18, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("1200-04-01");
    assertEquals(6, tm.getDayOfWeek());
    assertEquals(13, tm.getWeekOfYear());
    assertEquals(12, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("400-04-20");
    assertEquals(4, tm.getDayOfWeek());
    assertEquals(16, tm.getWeekOfYear());
    assertEquals(4, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("310-12-31");
    assertEquals(6, tm.getDayOfWeek());
    assertEquals(52, tm.getWeekOfYear());
    assertEquals(4, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("0080-02-29");
    assertEquals(4, tm.getDayOfWeek());
    assertEquals(9, tm.getWeekOfYear());
    assertEquals(1, tm.getCenturyOfEra());

    tm = DateTimeUtil.decodeDateTime("400-03-01 BC");
    assertEquals(4, tm.getDayOfWeek());
    assertEquals(9, tm.getWeekOfYear());
    assertEquals(-4, tm.getCenturyOfEra());
  }

  @Test
  public void testStrtoi() {
    StringBuilder sb = new StringBuilder();
    int intVal = 12345;
    String textVal = "test";

    int value = DateTimeUtil.strtoi(intVal + textVal, 0, sb);
    assertEquals(intVal, value);
    assertEquals(textVal, sb.toString());

    textVal = "";

    value = DateTimeUtil.strtoi(intVal + textVal, 0, sb);
    assertEquals(intVal, value);
    assertEquals(textVal, sb.toString());
  }

  @Test
  public void testGetCenturyOfEra() {
    assertEquals(1, DateTimeUtil.getCenturyOfEra(1));
    assertEquals(1, DateTimeUtil.getCenturyOfEra(100));
    assertEquals(2, DateTimeUtil.getCenturyOfEra(101));
    assertEquals(10, DateTimeUtil.getCenturyOfEra(1000));
    assertEquals(20, DateTimeUtil.getCenturyOfEra(1998));
    assertEquals(20, DateTimeUtil.getCenturyOfEra(1999));
    assertEquals(20, DateTimeUtil.getCenturyOfEra(2000));
    assertEquals(21, DateTimeUtil.getCenturyOfEra(2001));
    assertEquals(21, DateTimeUtil.getCenturyOfEra(2100));
    assertEquals(22, DateTimeUtil.getCenturyOfEra(2101));

    assertEquals(-6, DateTimeUtil.getCenturyOfEra(-600));
    assertEquals(-6, DateTimeUtil.getCenturyOfEra(-501));
    assertEquals(-5, DateTimeUtil.getCenturyOfEra(-500));
    assertEquals(-5, DateTimeUtil.getCenturyOfEra(-455));
    assertEquals(-1, DateTimeUtil.getCenturyOfEra(-1));
  }

  @Test
  public void testGetTimeZoneDisplayTime() {
    assertEquals("", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("GMT"), false));
    assertEquals("+09", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("GMT+9"), false));
    assertEquals("+09:10", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("GMT+9:10"), false));
    assertEquals("-09", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("GMT-9"), false));
    assertEquals("-09:10", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("GMT-9:10"), false));
    assertEquals("-07", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("America/Los_Angeles"), true));
    assertEquals("-08", DateTimeUtil.getDisplayTimeZoneOffset(TimeZone.getTimeZone("America/Los_Angeles"), false));
  }
  
  @Test
  public void testGetYear() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-01-01 00:00:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getYear(TEST_DATETIME));
  }

  @Test
  public void testGetMonth() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-01 00:00:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getMonth(TEST_DATETIME));
  }

  @Test
  public void testGetDay() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-18 00:00:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getDay(TEST_DATETIME));
  }
  
  @Test
  public void testGetDayOfWeek() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-14 00:00:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getDayOfWeek(TEST_DATETIME, DateTimeConstants.MONDAY));
    
    javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-15 00:00:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getDayOfWeek(TEST_DATETIME, DateTimeConstants.TUESDAY));
  }

  @Test
  public void testGetHour() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-18 01:00:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getHour(TEST_DATETIME));
  }

  @Test
  public void testGetMinute() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-18 01:15:00")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getMinute(TEST_DATETIME));
  }

  @Test
  public void testGetSecond() {
    long javaTimestamp = 
        DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp("2014-04-18 01:15:25")) *
        DateTimeConstants.USECS_PER_MSEC;
    assertEquals(javaTimestamp, DateTimeUtil.getSecond(TEST_DATETIME));
  }
  
  @Test
  public void testGetUTCDateTime() {
    long javaTimestamp = DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp(TEST_DATETIME)) *
        DateTimeConstants.USECS_PER_MSEC;
    javaTimestamp += (TEST_DATETIME.fsecs%DateTimeConstants.USECS_PER_MSEC);
    Int8Datum datum = DatumFactory.createInt8(javaTimestamp);
    
    assertTrue(TEST_DATETIME.equals(DateTimeUtil.getUTCDateTime(datum)));
  }
  
}
