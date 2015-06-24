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

import com.google.common.primitives.Ints;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

public class DateDatum extends Datum {
  public static final int SIZE = 4;

  // Dates are stored in UTC.
  final int year;
  final int monthOfYear;
  final int dayOfMonth;

  public DateDatum(int value) {
    this(DateTimeUtil.j2date(value));
  }

  public DateDatum(TimeMeta tm) {
    super(TajoDataTypes.Type.DATE);
    year = tm.years;
    monthOfYear = tm.monthOfYear;
    dayOfMonth = tm.dayOfMonth;
  }

  public TimeMeta asTimeMeta() {
    TimeMeta tm = new TimeMeta();
    tm.years = year;
    tm.monthOfYear = monthOfYear;
    tm.dayOfMonth = dayOfMonth;
    return tm;
  }

  public int getCenturyOfEra() {
    TimeMeta tm = asTimeMeta();
    return tm.getCenturyOfEra();
  }

  public int getYear() {
    return year;
  }

  public int getWeekyear() {
    TimeMeta tm = asTimeMeta();
    return tm.getWeekyear();
  }

  public int getMonthOfYear() {
    return monthOfYear;
  }

  public int getDayOfYear() {
    TimeMeta tm = asTimeMeta();
    return tm.getDayOfYear();
  }

  public int getDayOfWeek() {
    TimeMeta tm = asTimeMeta();
    return tm.getDayOfWeek();
  }

  public int getISODayOfWeek() {
    TimeMeta tm = asTimeMeta();
    return tm.getISODayOfWeek();
  }

  public int getWeekOfYear() {
    TimeMeta tm = asTimeMeta();
    return tm.getWeekOfYear();
  }

  public int getDayOfMonth() {
    return dayOfMonth;
  }

  @Override
  public String toString() {
    return asChars();
  }

  @Override
  public Datum plus(Datum datum) {
    switch(datum.type()) {
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: {
        TimeMeta tm = asTimeMeta();
        tm.plusDays(datum.asInt4());
        return new DateDatum(tm);
      }
      case INTERVAL:
        IntervalDatum interval = (IntervalDatum) datum;
        TimeMeta tm = asTimeMeta();
        tm.plusInterval(interval.months, interval.milliseconds);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      case TIME: {
        TimeMeta tm1 = asTimeMeta();
        TimeMeta tm2 = datum.asTimeMeta();
        tm1.plusTime(DateTimeUtil.toTime(tm2));
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm1));
      }
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum minus(Datum datum) {
    switch(datum.type()) {
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: {
        TimeMeta tm = asTimeMeta();
        tm.plusDays(0 - datum.asInt4());
        return new DateDatum(tm);
      }
      case INTERVAL: {
        IntervalDatum interval = (IntervalDatum) datum;
        TimeMeta tm = asTimeMeta();
        tm.plusInterval(-interval.months, -interval.milliseconds);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      }
      case TIME: {
        TimeMeta tm1 = asTimeMeta();
        TimeMeta tm2 = datum.asTimeMeta();
        tm1.plusTime(0 - DateTimeUtil.toTime(tm2));
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm1));
      }
      case DATE: {
        DateDatum d = (DateDatum) datum;
        int day1 = DateTimeUtil.date2j(year, monthOfYear, dayOfMonth);
        int day2 = DateTimeUtil.date2j(d.year, d.monthOfYear, d.dayOfMonth);
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
    return DateTimeUtil.encodeDate(year, monthOfYear, dayOfMonth, DateStyle.ISO_DATES);
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
      int compare = Ints.compare(year, another.year);
      if (compare != 0) {
        return compare;
      }
      compare = Ints.compare(monthOfYear, another.monthOfYear);
      if (compare != 0) {
        return compare;
      }
      return Ints.compare(dayOfMonth, another.dayOfMonth);
    } else if (datum.type() == TajoDataTypes.Type.TIMESTAMP) {
      TimestampDatum another = (TimestampDatum) datum;
      TimeMeta myMeta, otherMeta;
      myMeta = asTimeMeta();
      otherMeta = another.asTimeMeta();
      return myMeta.compareTo(otherMeta);
    } else if (datum instanceof NullDatum || datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
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
