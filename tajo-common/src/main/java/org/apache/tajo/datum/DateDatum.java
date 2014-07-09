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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

public class DateDatum extends Datum {
  public static final int SIZE = 4;

  private int year;
  private int monthOfYear;
  private int dayOfMonth;

  public DateDatum(int value) {
    super(TajoDataTypes.Type.DATE);
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.j2date(value, tm);

    year = tm.years;
    monthOfYear = tm.monthOfYear;
    dayOfMonth = tm.dayOfMonth;
  }

  public TimeMeta toTimeMeta() {
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.j2date(DateTimeUtil.date2j(year, monthOfYear, dayOfMonth), tm);
    return tm;
  }

  public int getCenturyOfEra() {
    TimeMeta tm = toTimeMeta();
    return tm.getCenturyOfEra();
  }

  public int getYear() {
    TimeMeta tm = toTimeMeta();
    return tm.years;
  }

  public int getWeekyear() {
    TimeMeta tm = toTimeMeta();
    return tm.getWeekyear();
  }

  public int getMonthOfYear() {
    TimeMeta tm = toTimeMeta();
    return tm.monthOfYear;
  }

  public int getDayOfYear() {
    TimeMeta tm = toTimeMeta();
    return tm.getDayOfYear();
  }

  public int getDayOfWeek() {
    TimeMeta tm = toTimeMeta();
    return tm.getDayOfWeek();
  }

  public int getISODayOfWeek() {
    TimeMeta tm = toTimeMeta();
    return tm.getISODayOfWeek();
  }

  public int getWeekOfYear() {
    TimeMeta tm = toTimeMeta();
    return tm.getWeekOfYear();
  }

  public int getDayOfMonth() {
    TimeMeta tm = toTimeMeta();
    return tm.dayOfMonth;
  }


  public String toString() {
    return asChars();
  }

  public Datum plus(Datum datum) {
    switch(datum.type()) {
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: {
        TimeMeta tm = toTimeMeta();
        tm.plusDays(datum.asInt4());
        return new DateDatum(DateTimeUtil.date2j(tm.years, tm.monthOfYear, tm.dayOfMonth));
      }
      case INTERVAL: {
        IntervalDatum interval = (IntervalDatum) datum;
        TimeMeta tm = toTimeMeta();
        tm.plusMillis(interval.getMilliSeconds());
        if (interval.getMonths() > 0) {
          tm.plusMonths(interval.getMonths());
        }
        DateTimeUtil.toUTCTimezone(tm);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      }
      case TIME: {
        TimeMeta tm1 = toTimeMeta();

        TimeMeta tm2 = ((TimeDatum)datum).toTimeMeta();
        DateTimeUtil.toUserTimezone(tm2);     //TimeDatum is UTC

        tm1.plusTime(DateTimeUtil.toTime(tm2));
        DateTimeUtil.toUTCTimezone(tm1);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm1));
      }
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  public Datum minus(Datum datum) {
    switch(datum.type()) {
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: {
        TimeMeta tm = toTimeMeta();
        tm.plusDays(0 - datum.asInt4());
        return new DateDatum(DateTimeUtil.date2j(tm.years, tm.monthOfYear, tm.dayOfMonth));
      }
      case INTERVAL: {
        IntervalDatum interval = (IntervalDatum) datum;
        TimeMeta tm = toTimeMeta();
        if (interval.getMonths() > 0) {
          tm.plusMonths(0 - interval.getMonths());
        }
        tm.plusMillis(0 - interval.getMilliSeconds());
        DateTimeUtil.toUTCTimezone(tm);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      }
      case TIME: {
        TimeMeta tm1 = toTimeMeta();

        TimeMeta tm2 = ((TimeDatum)datum).toTimeMeta();
        DateTimeUtil.toUserTimezone(tm2);     //TimeDatum is UTC

        tm1.plusTime(0 - DateTimeUtil.toTime(tm2));
        DateTimeUtil.toUTCTimezone(tm1);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm1));
      }
      case DATE: {
        TimeMeta tm1 = toTimeMeta();
        TimeMeta tm2 = ((DateDatum) datum).toTimeMeta();

        int day1 = DateTimeUtil.date2j(tm1.years, tm1.monthOfYear, tm1.dayOfMonth);
        int day2 = DateTimeUtil.date2j(tm2.years, tm2.monthOfYear, tm2.dayOfMonth);
        return new Int4Datum(day1 - day2);
      }
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int asInt4() {
    return encode();
  }

  private int encode() {
    return DateTimeUtil.date2j(year, monthOfYear, dayOfMonth);
  }

  @Override
  public long asInt8() {
    return encode();
  }

  @Override
  public float asFloat4() {
    throw new InvalidCastException();
  }

  @Override
  public double asFloat8() {
    throw new InvalidCastException();
  }

  @Override
  public String asChars() {
    TimeMeta tm = toTimeMeta();
    return DateTimeUtil.encodeDate(tm, DateStyle.ISO_DATES);
  }

  public String toChars(String format) {
    TimeMeta tm = toTimeMeta();
    return DateTimeFormat.to_char(tm, format);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(encode());
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == Type.DATE) {
      return DatumFactory.createBool(equals(datum));
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.DATE) {
      DateDatum another = (DateDatum) datum;
      int compareResult = (year < another.year) ? -1 : ((year == another.year) ? 0 : 1);
      if (compareResult != 0) {
        return compareResult;
      }
      compareResult = (monthOfYear < another.monthOfYear) ? -1 : ((monthOfYear == another.monthOfYear) ? 0 : 1);
      if (compareResult != 0) {
        return compareResult;
      }

      return (dayOfMonth < another.dayOfMonth) ? -1 : ((dayOfMonth == another.dayOfMonth) ? 0 : 1);
    } else if (datum instanceof NullDatum || datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof DateDatum) {
      DateDatum another = (DateDatum) obj;
      return year == another.year && monthOfYear == another.monthOfYear && dayOfMonth == another.dayOfMonth;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int total = 157;
    total = 23 * total + year;
    total = 23 * total + monthOfYear;
    total = 23 * total + dayOfMonth;

    return total;
  }
}
