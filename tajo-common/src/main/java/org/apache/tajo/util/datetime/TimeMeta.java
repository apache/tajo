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

package org.apache.tajo.util.datetime;

import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;

public class TimeMeta implements Comparable<TimeMeta> {
  public int      fsecs;    // 1/1,000,000 secs
  public int			secs;
  public int			minutes;
  public int			hours;
  public int			dayOfMonth;
  public int      dayOfYear;   //used for only DateTimeFormat
  public int			monthOfYear; // origin 0, not 1
  public int			years;		   // relative to 1900
  public boolean	isDST;       // daylight savings time
  public int      dayOfWeek;
  public int      timeZone = Integer.MAX_VALUE;   //sec, used for only Text -> Timestamp

  @Override
  public String toString() {
    return DateTimeUtil.encodeDateTime(this, DateStyle.ISO_DATES);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + dayOfMonth;
    result = prime * result + dayOfWeek;
    result = prime * result + dayOfYear;
    result = prime * result + fsecs;
    result = prime * result + hours;
    result = prime * result + (isDST ? 1231 : 1237);
    result = prime * result + minutes;
    result = prime * result + monthOfYear;
    result = prime * result + secs;
    result = prime * result + timeZone;
    result = prime * result + years;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeMeta)) {
      return false;
    }
    TimeMeta other = (TimeMeta)o;

    return fsecs == other.fsecs &&
        secs == other.secs &&
        minutes == other.minutes &&
        hours == other.hours &&
        dayOfMonth == other.dayOfMonth &&
        dayOfYear == other.dayOfYear &&
        monthOfYear == other.monthOfYear &&
        years == other.years &&
        isDST == other.isDST
        ;
  }

  public void plusMonths(int months) {
    if (months == 0) {
      return;
    }
    int thisYear = years;
    int thisMonth = monthOfYear;

    int yearToUse;
    // Initially, monthToUse is zero-based
    int monthToUse = thisMonth - 1 + months;
    if (monthToUse >= 0) {
      yearToUse = thisYear + (monthToUse / DateTimeConstants.MONTHS_PER_YEAR);
      monthToUse = (monthToUse % DateTimeConstants.MONTHS_PER_YEAR) + 1;
    } else {
      yearToUse = thisYear + (monthToUse / DateTimeConstants.MONTHS_PER_YEAR) - 1;
      monthToUse = Math.abs(monthToUse);
      int remMonthToUse = monthToUse % DateTimeConstants.MONTHS_PER_YEAR;
      // Take care of the boundary condition
      if (remMonthToUse == 0) {
        remMonthToUse = DateTimeConstants.MONTHS_PER_YEAR;
      }
      monthToUse = DateTimeConstants.MONTHS_PER_YEAR - remMonthToUse + 1;
      // Take care MONTHS_PER_YEAR the boundary condition
      if (monthToUse == 1) {
        yearToUse += 1;
      }
    }
    // End of do not refactor.
    // ----------------------------------------------------------

    //
    // Quietly force DOM to nearest sane value.
    //
    int dayToUse = dayOfMonth;
    int maxDay = DateTimeUtil.getDaysInYearMonth(yearToUse, monthToUse);
    if (dayToUse > maxDay) {
      dayToUse = maxDay;
    }

    this.years = yearToUse;
    this.monthOfYear = monthToUse;
    this.dayOfMonth = dayToUse;
  }

  public void plusDays(int days) {
    long timestamp = DateTimeUtil.toJulianTimestamp(this);
    timestamp += days * DateTimeConstants.USECS_PER_DAY;

    DateTimeUtil.toJulianTimeMeta(timestamp, this);
  }

  public void plusMillis(long millis) {
    plusTime(millis * 1000);
  }

  public void plusTime(long time) {
    long timestamp = DateTimeUtil.toJulianTimestamp(this);
    timestamp += time;
    DateTimeUtil.toJulianTimeMeta(timestamp, this);
  }

  public void plusInterval(int months, long milliseconds) {
    if (months != 0) {
      plusMonths(months);
    }
    if (milliseconds != 0) {
      plusMillis(milliseconds);
    }
  }

  public int getCenturyOfEra() {
    return DateTimeUtil.getCenturyOfEra(years);
  }

  public int getDayOfYear() {
    int dayOfYear = 0;
    for (int i = 0; i < monthOfYear - 1; i++) {
      dayOfYear += DateTimeUtil.getDaysInYearMonth(years, i + 1);
    }

    return dayOfYear + dayOfMonth;
  }

  public int getWeekOfYear() {
    return DateTimeUtil.date2isoweek(years, monthOfYear, dayOfMonth);
  }

  public int getWeekyear() {
    return DateTimeUtil.date2isoyear(years, monthOfYear, dayOfMonth);
  }

  public int getISODayOfWeek() {
    int dow = getDayOfWeek();
    if (dow == 0) {   //Sunday
      return 7;
    } else {
      return dow;
    }
  }

  public int getDayOfWeek() {
    return (DateTimeUtil.date2j(years, monthOfYear, dayOfMonth) + 1) % 7;
  }

  @Override
  public int compareTo(TimeMeta o) {
    int result = 1;
    if (o != null) {
      long leftjulianTimestamp = DateTimeUtil.toJulianTimestamp(this);
      long rightjulianTimestamp = DateTimeUtil.toJulianTimestamp(o);
      
      result = (int) Math.signum(leftjulianTimestamp - rightjulianTimestamp);
    }
    return result;
  }
}