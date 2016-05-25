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

import com.google.common.primitives.Longs;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

import static org.apache.tajo.type.Type.Timestamp;

public class TimestampDatum extends Datum {
  public static final int SIZE = 8;

  private long timestamp;

  /**
   *
   * @param timestamp UTC based Julian time microseconds
   */
  public TimestampDatum(long timestamp) {
    super(Timestamp);
    this.timestamp = timestamp;
  }

  /**
   * It's the same value to asInt8().
   * @return The Timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  public int getEpoch() {
    return DateTimeUtil.julianTimeToEpoch(timestamp);
  }

  public long getJavaTimestamp() {
    return DateTimeUtil.julianTimeToJavaTime(timestamp);
  }


  public int getCenturyOfEra() {
    TimeMeta tm = asTimeMeta();
    return tm.getCenturyOfEra();
  }

  public int getYear() {
    TimeMeta tm = asTimeMeta();
    return tm.years;
  }

  public int getMonthOfYear() {
    TimeMeta tm = asTimeMeta();
    return tm.monthOfYear;
  }

  public int getDayOfYear() {
    TimeMeta tm = asTimeMeta();
    return tm.getDayOfYear();
  }

  public int getDayOfWeek() {
    TimeMeta tm = asTimeMeta();
    return tm.getDayOfYear();
  }

  public int getWeekOfYear() {
    TimeMeta tm = asTimeMeta();
    return tm.getWeekOfYear();
  }

  public int getDayOfMonth() {
    TimeMeta tm = asTimeMeta();
    return tm.dayOfMonth;
  }

  public int getHourOfDay() {
    TimeMeta tm = asTimeMeta();
    return tm.hours;
  }

  public int getMinuteOfHour() {
    TimeMeta tm = asTimeMeta();
    return tm.minutes;
  }

  public int getSecondOfMinute() {
    TimeMeta tm = asTimeMeta();
    return tm.secs;
  }

  public int getMillisOfSecond() {
    TimeMeta tm = asTimeMeta();
    return tm.fsecs / 1000;
  }

  public int getUnixTime() {
    return (int)(DateTimeUtil.julianTimeToJavaTime(timestamp) / 1000);
  }

  public String toString() {
    return asChars();
  }

  /**
   *
   * @param tm TimeMeta
   * @param timeZone Timezone
   * @param includeTimeZone Add timezone if it is true. It is usually used for TIMEZONEZ
   * @return A timestamp string
   */
  public static String asChars(TimeMeta tm, TimeZone timeZone, boolean includeTimeZone) {
    DateTimeUtil.toUserTimezone(tm, timeZone);
    if (includeTimeZone) {
      tm.timeZone = tm.getZonedOffset(DateTimeUtil.toJulianTimestamp(tm), timeZone) / 1000;
    }
    return DateTimeUtil.encodeDateTime(tm, DateStyle.ISO_DATES);
  }

  public String toString(TimeZone timeZone, boolean includeTimeZone) {
    return asChars(asTimeMeta(), timeZone, includeTimeZone);
  }

  @Override
  public long asInt8() {
    return timestamp;
  }

  @Override
  public String asChars() {
    TimeMeta tm = asTimeMeta();
    return asChars(tm, TimeZone.getDefault(), true);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(timestamp);
  }

  @Override
  public byte[] asTextBytes() {
    return asChars().getBytes(TextDatum.DEFAULT_CHARSET);
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.kind() == TajoDataTypes.Type.TIMESTAMP) {
      return timestamp == datum.asInt8() ? BooleanDatum.TRUE : BooleanDatum.FALSE;
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.kind() == TajoDataTypes.Type.TIMESTAMP) {
      TimestampDatum another = (TimestampDatum) datum;
      return Longs.compare(timestamp, another.timestamp);
    } else if (datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TimestampDatum) {
      TimestampDatum another = (TimestampDatum) obj;
      return timestamp == another.timestamp;
    } else {
      return false;
    }
  }

  @Override
  public Datum plus(Datum datum) {
    switch (datum.kind()) {
    case INTERVAL:
      IntervalDatum interval = (IntervalDatum) datum;
      TimeMeta tm = asTimeMeta();
      tm.plusInterval(interval.months, interval.milliseconds);
      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
    case TIME:
      TimeMeta tm1 = asTimeMeta();
      TimeMeta tm2 = datum.asTimeMeta();
      tm1.plusTime(DateTimeUtil.toTime(tm2));
      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm1));
    default:
      throw new InvalidOperationException("operator does not exist: " + type() + " + " + datum.type());
    }
  }

  @Override
  public Datum minus(Datum datum) {
    switch (datum.kind()) {
    case INTERVAL:
      IntervalDatum interval = (IntervalDatum) datum;
      TimeMeta tm = asTimeMeta();
      tm.plusInterval(-interval.months, -interval.milliseconds);
      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
    case TIMESTAMP:
      return new IntervalDatum((timestamp - ((TimestampDatum) datum).timestamp) / 1000);
    case TIME:
      TimeMeta tm1 = asTimeMeta();
      TimeMeta tm2 = datum.asTimeMeta();
      tm1.plusTime(0 - DateTimeUtil.toTime(tm2));
      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm1));
    default:
      throw new InvalidOperationException("operator does not exist: " + type() + " - " + datum.type());
    }
  }

  @Override
  public int hashCode(){
    return Longs.hashCode(timestamp);
  }

  @Override
  public TimeMeta asTimeMeta() {
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.toJulianTimeMeta(timestamp, tm);

    return tm;
  }
}
