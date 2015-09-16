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
import org.apache.tajo.exception.InvalidValueForCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

public class DateDatum extends Datum {
  public static final int SIZE = 4;

  // Dates are stored in UTC.
  private int jdate;

  public DateDatum(int value) {
    super(TajoDataTypes.Type.DATE);

    jdate = value;
  }

  public DateDatum(TimeMeta tm) {
    super(TajoDataTypes.Type.DATE);
    jdate = DateTimeUtil.date2j(tm.years, tm.monthOfYear, tm.dayOfMonth);
  }

  public TimeMeta asTimeMeta() {
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.j2date(jdate, tm);

    return tm;
  }

  public int getCenturyOfEra() {
    return asTimeMeta().getCenturyOfEra();
  }

  public int getYear() {
    return asTimeMeta().years;
  }

  public int getWeekyear() {
    return asTimeMeta().getWeekyear();
  }

  public int getMonthOfYear() {
    return asTimeMeta().monthOfYear;
  }

  public int getDayOfYear() {
    return asTimeMeta().getDayOfYear();
  }

  public int getDayOfWeek() {
    return asTimeMeta().getDayOfWeek();
  }

  public int getISODayOfWeek() {
    return asTimeMeta().getISODayOfWeek();
  }

  public int getWeekOfYear() {
    return asTimeMeta().getWeekOfYear();
  }

  public int getDayOfMonth() {
    return asTimeMeta().dayOfMonth;
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
        return new Int4Datum(jdate - d.jdate);
      }
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int asInt4() {
    return jdate;
  }

  @Override
  public long asInt8() {
    return jdate;
  }

  @Override
  public float asFloat4() {
    throw new TajoRuntimeException(new InvalidValueForCastException(Type.DATE, Type.FLOAT4));
  }

  @Override
  public double asFloat8() {
    throw new TajoRuntimeException(new InvalidValueForCastException(Type.DATE, Type.FLOAT8));
  }

  @Override
  public String asChars() {
    return DateTimeUtil.encodeDate(asTimeMeta(), DateStyle.ISO_DATES);
  }

  public String toChars(String format) {
    return DateTimeFormat.to_char(asTimeMeta(), format);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(jdate);
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
      return Ints.compare(jdate, another.jdate);
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
    TimeMeta tm = asTimeMeta();
    if (obj instanceof DateDatum) {
      TimeMeta another = ((DateDatum) obj).asTimeMeta();
      return tm.years == another.years && tm.monthOfYear == another.monthOfYear && tm.dayOfMonth == another.dayOfMonth;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    TimeMeta tm = asTimeMeta();
    int total = 157;
    total = 23 * total + tm.years;
    total = 23 * total + tm.monthOfYear;
    total = 23 * total + tm.dayOfMonth;

    return total;
  }
}
