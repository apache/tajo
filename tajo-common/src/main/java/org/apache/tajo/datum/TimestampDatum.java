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

import com.google.common.base.Objects;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

public class TimestampDatum extends Datum {
  public static final int SIZE = 8;

  private long timestamp;

  /**
   *
   * @param timestamp UTC based
   */
  public TimestampDatum(long timestamp) {
    super(TajoDataTypes.Type.TIMESTAMP);
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
    TimeMeta tm = toTimeMeta();
    return tm.getCenturyOfEra();
  }

  public int getYear() {
    TimeMeta tm = toTimeMeta();
    return tm.years;
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
    return tm.getDayOfYear();
  }

  public int getWeekOfYear() {
    TimeMeta tm = toTimeMeta();
    return tm.getWeekOfYear();
  }

  public int getDayOfMonth() {
    TimeMeta tm = toTimeMeta();
    return tm.dayOfMonth;
  }

  public int getHourOfDay() {
    TimeMeta tm = toTimeMeta();
    return tm.hours;
  }

  public int getMinuteOfHour() {
    TimeMeta tm = toTimeMeta();
    return tm.minutes;
  }

  public int getSecondOfMinute() {
    TimeMeta tm = toTimeMeta();
    return tm.secs;
  }

  public int getMillisOfSecond() {
    TimeMeta tm = toTimeMeta();
    return tm.fsecs / 1000;
  }

  public int getUnixTime() {
    return (int)(DateTimeUtil.julianTimeToJavaTime(timestamp) / 1000);
  }

  public String toString() {
    return asChars();
  }

  public String asChars(TimeZone timeZone, boolean includeTimeZone) {
    TimeMeta tm = toTimeMeta();
    DateTimeUtil.toUserTimezone(tm, timeZone);
    if (includeTimeZone) {
      tm.timeZone = timeZone.getRawOffset() / 1000;
    }
    return  DateTimeUtil.encodeDateTime(tm, DateStyle.ISO_DATES);
  }

  public String toString(TimeZone timeZone, boolean includeTimeZone) {
    return asChars(timeZone, includeTimeZone);
  }

  @Override
  public long asInt8() {
    return timestamp;
  }

  @Override
  public String asChars() {
    TimeMeta tm = toTimeMeta();
    return DateTimeUtil.encodeDateTime(tm, DateStyle.ISO_DATES);
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
    return Bytes.toBytes(timestamp);
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return timestamp == datum.asInt8() ? BooleanDatum.TRUE : BooleanDatum.FALSE;
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIMESTAMP) {
      TimestampDatum another = (TimestampDatum) datum;
      return (timestamp < another.timestamp) ? -1 : ((timestamp > another.timestamp) ? 1 : 0);
    } else if (datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

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
    if (datum.type() == TajoDataTypes.Type.INTERVAL) {
      IntervalDatum interval = (IntervalDatum)datum;

      TimeMeta tm = new TimeMeta();
      DateTimeUtil.toJulianTimeMeta(timestamp, tm);

      if (interval.getMonths() > 0) {
        tm.plusMonths(interval.getMonths());
      }
      if (interval.getMilliSeconds() > 0) {
        tm.plusMillis(interval.getMilliSeconds());
      }

      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));

    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum minus(Datum datum) {
    switch(datum.type()) {
      case INTERVAL:
        IntervalDatum interval = (IntervalDatum)datum;

        TimeMeta tm = new TimeMeta();
        DateTimeUtil.toJulianTimeMeta(timestamp, tm);

        if (interval.getMonths() > 0) {
          tm.plusMonths(0 - interval.getMonths());
        }
        if (interval.getMilliSeconds() > 0) {
          tm.plusMillis(0 - interval.getMilliSeconds());
        }
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      case TIMESTAMP:
        return new IntervalDatum((timestamp - ((TimestampDatum)datum).timestamp) / 1000);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int hashCode(){
     return Objects.hashCode(timestamp);
  }

  public TimeMeta toTimeMeta() {
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.toJulianTimeMeta(timestamp, tm);

    return tm;
  }
}
