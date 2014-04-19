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

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTimeStampUtil {
  private static final int TEST_YEAR = 2014;
  private static final int TEST_MONTH_OF_YEAR = 4;
  private static final int TEST_DAY_OF_MONTH = 18;
  private static final int TEST_HOUR_OF_DAY = 0;
  private static final int TEST_MINUTE_OF_HOUR = 15;
  private static final int TEST_SECOND_OF_MINUTE = 25;
  private static final DateTime TEST_DATETIME = new DateTime(TEST_YEAR, TEST_MONTH_OF_YEAR, TEST_DAY_OF_MONTH,
      TEST_HOUR_OF_DAY, TEST_MINUTE_OF_HOUR, TEST_SECOND_OF_MINUTE, DateTimeZone.UTC);
  private static final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  @Test
  public void testGetYear() {
    assertEquals(DateTime.parse("2014-01-01 00:00:00", fmt.withZoneUTC()).getMillis() * 1000,
        TimeStampUtil.getYear(TEST_DATETIME));
  }

  @Test
  public void testGetMonth() {
    assertEquals(DateTime.parse("2014-04-01 00:00:00", fmt.withZoneUTC()).getMillis() * 1000,
        TimeStampUtil.getMonth(TEST_DATETIME));
  }

  @Test
  public void testGetDay() {
    assertEquals(DateTime.parse("2014-04-18 00:00:00", fmt.withZoneUTC()).getMillis() * 1000,
        TimeStampUtil.getDay(TEST_DATETIME));
  }

  @Test
  public void testGetHour() {
    assertEquals(DateTime.parse("2014-04-18 00:00:00",fmt.withZoneUTC()).getMillis() * 1000,
        TimeStampUtil.getHour(TEST_DATETIME));
  }

  @Test
  public void testGetMinute() {
    assertEquals(DateTime.parse("2014-04-18 00:15:00",fmt.withZoneUTC()).getMillis() * 1000,
        TimeStampUtil.getMinute(TEST_DATETIME));
  }

  @Test
  public void testGetSecond() {
    assertEquals(DateTime.parse("2014-04-18 00:15:25",fmt.withZoneUTC()).getMillis() * 1000,
        TimeStampUtil.getSecond(TEST_DATETIME));
  }
}
