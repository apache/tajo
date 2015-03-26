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

import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.TimeMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestDateTimeFormat {
  @Test
  public void testToTimestamp() {
    evalToTimestampAndAssert("1997-12-30 11:40:50.345", "YYYY-MM-DD HH24:MI:SS.MS", 1997, 12, 30, 11, 40, 50, 345);
    evalToTimestampAndAssert("1997-12-30 11:40:50.345 PM", "YYYY-MM-DD HH24:MI:SS.MS PM", 1997, 12, 30, 23, 40, 50, 345);
    evalToTimestampAndAssert("0097/Feb/16 --> 08:14:30", "YYYY/Mon/DD --> HH:MI:SS", 97, 2, 16, 8, 14, 30, 0);
    evalToTimestampAndAssert("97/2/16 8:14:30", "FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS", 97, 2, 16, 8, 14, 30, 0);
    evalToTimestampAndAssert("1985 September 12", "YYYY FMMonth DD", 1985, 9, 12, 0, 0, 0, 0);
    evalToTimestampAndAssert("1,582nd VIII 21", "Y,YYYth FMRM DD", 1582, 8, 21, 0, 0, 0, 0);
    evalToTimestampAndAssert("05121445482000", "MMDDHH24MISSYYYY", 2000, 5, 12, 14, 45, 48, 0);
    evalToTimestampAndAssert("2000January09Sunday", "YYYYFMMonthDDFMDay", 2000, 1, 9, 0, 0, 0, 0);
    evalToTimestampAndAssert("97/Feb/16", "YY/Mon/DD", 1997, 2, 16, 0, 0, 0, 0);
    evalToTimestampAndAssert("19971116", "YYYYMMDD", 1997, 11, 16, 0, 0, 0, 0);
    evalToTimestampAndAssert("20000-1116", "YYYY-MMDD", 20000, 11, 16, 0, 0, 0, 0);
    evalToTimestampAndAssert("9-1116", "Y-MMDD", 2009, 11, 16, 0, 0, 0, 0);
    evalToTimestampAndAssert("95-1116", "YY-MMDD", 1995, 11, 16, 0, 0, 0, 0);
    evalToTimestampAndAssert("995-1116", "YYY-MMDD", 1995, 11, 16, 0, 0, 0, 0);
    evalToTimestampAndAssert("2005426", "YYYYWWD", 2005, 10, 15, 0, 0, 0, 0);
    evalToTimestampAndAssert("2005300", "YYYYDDD", 2005, 10, 27, 0, 0, 0, 0);
    evalToTimestampAndAssert("2005527", "IYYYIWID", 2006, 1, 1, 0, 0, 0, 0);
    evalToTimestampAndAssert("005527", "IYYIWID", 2006, 1, 1, 0, 0, 0, 0);
    evalToTimestampAndAssert("05527", "IYIWID", 2006, 1, 1, 0, 0, 0, 0);
    evalToTimestampAndAssert("5527", "IIWID", 2006, 1, 1, 0, 0, 0, 0);
    evalToTimestampAndAssert("2005364", "IYYYIDDD", 2006, 1, 1, 0, 0, 0, 0);
    evalToTimestampAndAssert("20050302", "YYYYMMDD", 2005, 3, 2, 0, 0, 0, 0);
    evalToTimestampAndAssert("2005 03 02", "YYYYMMDD", 2005, 3, 2, 0, 0, 0, 0);
    evalToTimestampAndAssert(" 2005 03 02", "YYYYMMDD", 2005, 3, 2, 0, 0, 0, 0);
    evalToTimestampAndAssert("  20050302", "YYYYMMDD", 2005, 3, 2, 0, 0, 0, 0);

    //In the case of using Format Cache
    evalToTimestampAndAssert("1998-02-28 10:20:30.123 PM", "YYYY-MM-DD HH24:MI:SS.MS PM", 1998, 2, 28, 22, 20, 30, 123);


    try {
      evalToTimestampAndAssert("97/Feb/16", "YYMonDD", 1997, 2, 16, 0, 0, 0, 0);
      fail("Should be throw exception");
    } catch (Exception e) {
      //Invalid value /Fe for Mon. The given value did not match any of the allowed values for this field.
    }

    try {
      evalToTimestampAndAssert("2005527", "YYYYIWID", 2005, 5, 27, 0, 0, 0, 0);
      fail("Should be throw exception");
    } catch (Exception e) {
      //Do not mix Gregorian and ISO week date conventions in a formatting template.
    }
    try {
      evalToTimestampAndAssert("19971", "YYYYMMDD", 1997, 1, 1, 0, 0, 0, 0);
      fail("Should be throw exception");
    } catch (Exception e) {
      //If your source string is not fixed-width, try using the "FM" modifier.
    }

    TimeMeta tm = DateTimeFormat.parseDateTime("10:09:37.5", "HH24:MI:SS.MS");
    assertEquals(10, tm.hours);
  }

  @Test
  public void testToChar() {
    evalToCharAndAssert("1970-01-17 10:09:37", "YYYY-MM-DD HH24:MI:SS", "1970-01-17 10:09:37");
    evalToCharAndAssert("1997-12-30 11:40:50.345", "YYYY-MM-DD HH24:MI:SS.MS", "1997-12-30 11:40:50.345");
    evalToCharAndAssert("1997-12-30 11:40:50.345 PM", "YYYY-MM-DD HH24:MI:SS.MS PM", "1997-12-30 23:40:50.345 PM");
    evalToCharAndAssert("0097/Feb/16 --> 08:14:30", "YYYY/Mon/DD --> HH:MI:SS", "0097/Feb/16 --> 08:14:30");
    evalToCharAndAssert("97/2/16 8:14:30", "FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS", "97/2/16 8:14:30");
    evalToCharAndAssert("1985 September 12", "YYYY FMMonth DD", "1985 September 12");
    evalToCharAndAssert("1,582nd VIII 21", "Y,YYYth FMRM DD", "1,582nd VIII 21");
    evalToCharAndAssert("05121445482000", "MMDDHH24MISSYYYY", "05121445482000");
    evalToCharAndAssert("2000January09Sunday", "YYYYFMMonthDDFMDay", "2000January09Sunday");
    evalToCharAndAssert("97/Feb/16", "YY/Mon/DD", "97/Feb/16");
    evalToCharAndAssert("19971116", "YYYYMMDD", "19971116");
    evalToCharAndAssert("20000-1116", "YYYY-MMDD", "20000-1116");
    evalToCharAndAssert("9-1116", "Y-MMDD", "9-1116");
    evalToCharAndAssert("95-1116", "YY-MMDD", "95-1116");
    evalToCharAndAssert("995-1116", "YYY-MMDD", "995-1116");
    evalToCharAndAssert("2005426", "YYYYWWD", "2005427");
    evalToCharAndAssert("2005300", "YYYYDDD", "2005300");
    evalToCharAndAssert("2005527", "IYYYIWID", "2005527");
    evalToCharAndAssert("005527", "IYYIWID", "005527");
    evalToCharAndAssert("05527", "IYIWID", "05527");
    evalToCharAndAssert("5527", "IIWID", "5527");
    evalToCharAndAssert("2005364", "IYYYIDDD", "2005364");
    evalToCharAndAssert("20050302", "YYYYMMDD", "20050302");
    evalToCharAndAssert("2005 03 02", "YYYYMMDD", "YYYY MM DD", "2005 03 02");
    evalToCharAndAssert(" 2005 03 02", "YYYYMMDD", " YYYY MM DD", " 2005 03 02");
    evalToCharAndAssert("  20050302", "YYYYMMDD", "  YYYYMMDD", "  20050302");
  }

  @Test
  public void testPerformance() {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 10000000; i++) {
      DateTimeFormat.toTimestamp("1997-12-30 11:40:50.345", "YYYY-MM-DD HH24:MI:SS.MS");
    }
    long endTime = System.currentTimeMillis();
    System.out.println("total parse time with TajoDateTimeFormat:" + (endTime - startTime) + " ms");
  }

  private void evalToTimestampAndAssert(String dateTimeText, String formatText,
                                        int year, int month, int day, int hour, int minute, int sec, int msec) {
    TimestampDatum datum = DateTimeFormat.toTimestamp(dateTimeText, formatText);
    assertEquals(year, datum.getYear());
    assertEquals(month, datum.getMonthOfYear());
    assertEquals(day, datum.getDayOfMonth());
    assertEquals(hour, datum.getHourOfDay());
    assertEquals(minute, datum.getMinuteOfHour());
    assertEquals(sec, datum.getSecondOfMinute());
    assertEquals(msec, datum.getMillisOfSecond());
  }

  private void evalToCharAndAssert(String dateTimeText, String toTimestampFormatText, String expected) {
    evalToCharAndAssert(dateTimeText, toTimestampFormatText, toTimestampFormatText, expected);
  }

  private void evalToCharAndAssert(String dateTimeText,
                                   String toTimestampFormatText, String toCharFormatText, String expected) {
    TimestampDatum datum = DateTimeFormat.toTimestamp(dateTimeText, toTimestampFormatText);
    String toCharResult = DateTimeFormat.to_char(datum.asTimeMeta(), toCharFormatText);
    assertEquals(expected, toCharResult);
  }
}
