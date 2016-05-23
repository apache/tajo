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
import org.apache.tajo.exception.InvalidValueForCastException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import static org.apache.tajo.type.Type.Time;

public class TimeDatum extends Datum {
  public static final int SIZE = 8;
  private final long time;

  public TimeDatum(long time) {
    super(Time);
    this.time = time;
  }

  @Override
  public TimeMeta asTimeMeta() {
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.date2j(time, tm);

    return tm;
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
    TimeMeta tm = new TimeMeta();
    DateTimeUtil.date2j(time, tm);
    return tm.secs;
  }

  public int getMillisOfSecond() {
    TimeMeta tm = asTimeMeta();
    return tm.fsecs / 1000;
  }

  public String toString() {
    return asChars();
  }

  @Override
  public int asInt4() {
    throw new TajoRuntimeException(new InvalidValueForCastException(TajoDataTypes.Type.TIME, TajoDataTypes.Type.INT4));
  }

  @Override
  public long asInt8() {
    return time;
  }

  @Override
  public float asFloat4() {
    throw new TajoRuntimeException(new InvalidValueForCastException(TajoDataTypes.Type.TIME, TajoDataTypes.Type.FLOAT4));
  }

  @Override
  public double asFloat8() {
    throw new TajoRuntimeException(new InvalidValueForCastException(TajoDataTypes.Type.TIME, TajoDataTypes.Type.FLOAT8));
  }

  @Override
  public String asChars() {
    TimeMeta tm = asTimeMeta();
    return DateTimeUtil.encodeTime(tm, DateStyle.ISO_DATES);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(asInt8());
  }

  @Override
  public Datum plus(Datum datum) {
    switch (datum.kind()) {
    case INTERVAL: {
      IntervalDatum interval = ((IntervalDatum) datum);
      TimeMeta tm = asTimeMeta();
      tm.plusInterval(interval.months, interval.milliseconds);
      return new TimeDatum(DateTimeUtil.toTime(tm));
    }
    case DATE: {
      DateDatum dateDatum = (DateDatum) datum;
      TimeMeta dateTm = dateDatum.asTimeMeta();
      dateTm.plusTime(time);
      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(dateTm));
    }
    case TIMESTAMP: {
      TimestampDatum timestampDatum = (TimestampDatum) datum;
      TimeMeta tm = timestampDatum.asTimeMeta();
      tm.plusTime(time);
      return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
    }
    default:
      throw new InvalidOperationException("operator does not exist: " + type() + " + " + datum.type());
    }
  }

  @Override
  public Datum minus(Datum datum) {
    switch(datum.kind()) {
      case INTERVAL:
        IntervalDatum interval = ((IntervalDatum)datum);
        TimeMeta tm = asTimeMeta();
        tm.plusInterval(-interval.months, -interval.milliseconds);
        return new TimeDatum(DateTimeUtil.toTime(tm));
      case TIME:
        return new IntervalDatum((time - ((TimeDatum)datum).time)/1000);
      default:
        throw new InvalidOperationException("operator does not exist: " + type() + " - " + datum.type());
    }
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.kind() == TajoDataTypes.Type.TIME) {
      return DatumFactory.createBool(time == (((TimeDatum) datum).time));
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.kind() == TajoDataTypes.Type.TIME) {
      TimeDatum another = (TimeDatum)datum;
      return Longs.compare(time, another.time);
    } else if (datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
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
    return Longs.hashCode(time);
  }

}
