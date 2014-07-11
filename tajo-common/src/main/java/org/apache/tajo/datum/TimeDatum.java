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
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

public class TimeDatum extends Datum {
  public static final int SIZE = 8;
  private final long time;

  public TimeDatum(long time) {
    super(TajoDataTypes.Type.TIME);
    this.time = time;
  }

  public TimeMeta toTimeMeta() {
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.date2j(time, tm);

    return tm;
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
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.date2j(time, tm);
    return tm.secs;
  }

  public int getMillisOfSecond() {
    TimeMeta tm = toTimeMeta();
    return tm.fsecs / 1000;
  }

  public String toString() {
    return asChars();
  }

  @Override
  public int asInt4() {
    throw new InvalidCastException();
  }

  @Override
  public long asInt8() {
    return time;
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
    return DateTimeUtil.encodeTime(tm, DateStyle.ISO_DATES);
  }

  public String asChars(TimeZone timeZone, boolean includeTimeZone) {
    TimeMeta tm = toTimeMeta();
    DateTimeUtil.toUserTimezone(tm, timeZone);
    if (includeTimeZone) {
      tm.timeZone = timeZone.getRawOffset() / 1000;
    }
    return DateTimeUtil.encodeTime(tm, DateStyle.ISO_DATES);
  }

  public String toString(TimeZone timeZone, boolean includeTimeZone) {
    return asChars(timeZone, includeTimeZone);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(asInt8());
  }

  public Datum plus(Datum datum) {
    switch(datum.type()) {
      case INTERVAL: {
        IntervalDatum interval = ((IntervalDatum)datum);
        TimeMeta tm = toTimeMeta();
        tm.plusMillis(interval.getMilliSeconds());
        return new TimeDatum(DateTimeUtil.toTime(tm));
      }
      case DATE: {
        TimeMeta tm = toTimeMeta();
        DateTimeUtil.toUserTimezone(tm);     //TimeDatum is UTC

        DateDatum dateDatum = (DateDatum) datum;
        TimeMeta dateTm = dateDatum.toTimeMeta();
        dateTm.plusTime(DateTimeUtil.toTime(tm));

        DateTimeUtil.toUTCTimezone(dateTm);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(dateTm));
      }
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  public Datum minus(Datum datum) {
    switch(datum.type()) {
      case INTERVAL: {
        IntervalDatum interval = ((IntervalDatum)datum);
        TimeMeta tm = toTimeMeta();
        tm.plusMillis(0 - interval.getMilliSeconds());
        return new TimeDatum(DateTimeUtil.toTime(tm));
      }
      case TIME:
        TimeMeta tm1 = toTimeMeta();
        TimeMeta tm2 = ((TimeDatum)datum).toTimeMeta();

        return new IntervalDatum((DateTimeUtil.toTime(tm1) - DateTimeUtil.toTime(tm2))/1000);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return DatumFactory.createBool(time == (((TimeDatum) datum).time));
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      TimeDatum another = (TimeDatum)datum;
      return (time < another.time) ? -1 : ((time == another.time) ? 0 : 1);
    } else if (datum instanceof NullDatum || datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof TimeDatum) {
      TimeDatum another = (TimeDatum) obj;
      return time == another.time;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return (int)(time ^ (time >>> 32));
  }

}
