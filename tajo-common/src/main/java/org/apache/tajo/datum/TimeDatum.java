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
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimeDatum extends Datum {
  public static final int SIZE = 8;
  /** ISO 8601/SQL standard format - ex) 07:37:16-08 */
  public static final String DEFAULT_FORMAT_STRING = "HH:mm:ss";
  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormat.forPattern(DEFAULT_FORMAT_STRING);
  private final LocalTime time;

  public TimeDatum(long value) {
    super(TajoDataTypes.Type.TIME);
    time = new LocalTime(value);
  }

  public TimeDatum(int hour, int minute, int second) {
    super(TajoDataTypes.Type.TIME);
    time = new LocalTime(hour, minute, second);
  }

  public TimeDatum(int hour, int minute, int second, int millis) {
    super(TajoDataTypes.Type.TIME);
    time = new LocalTime(hour, minute, second, millis);
  }

  public TimeDatum(String timeStr) {
    super(TajoDataTypes.Type.TIME);
    time = LocalTime.parse(timeStr, DEFAULT_FORMATTER);
  }

  public TimeDatum(LocalTime time) {
    super(TajoDataTypes.Type.TIME);
    this.time = time;
  }

  public TimeDatum(byte [] bytes) {
    this(Bytes.toLong(bytes));
  }

  public int getHourOfDay() {
    return time.getHourOfDay();
  }

  public int getMinuteOfHour() {
    return time.getMinuteOfHour();
  }

  public int getSecondOfMinute() {
    return time.getSecondOfMinute();
  }

  public int getMillisOfDay() {
    return time.getMillisOfDay();
  }

  public int getMillisOfSecond() {
    return time.getMillisOfSecond();
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
    return time.toDateTimeToday().getMillis();
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
    return time.toString(DEFAULT_FORMATTER);
  }

  public String toChars(String format) {
    return time.toString(format);
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
      case INTERVAL:
        IntervalDatum interval = ((IntervalDatum)datum);
        return new TimeDatum(time.plusMillis((int)interval.getMilliSeconds()));
      case DATE:
        DateTime dateTime = DateDatum.createDateTime(((DateDatum)datum).getDate(), time, true);
        return new TimestampDatum(dateTime);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  public Datum minus(Datum datum) {
    switch(datum.type()) {
      case INTERVAL:
        IntervalDatum interval = ((IntervalDatum)datum);
        return new TimeDatum(time.minusMillis((int)interval.getMilliSeconds()));
      case TIME:
        return new IntervalDatum(
            time.toDateTimeToday().getMillis() - ((TimeDatum)datum).getTime().toDateTimeToday().getMillis() );
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return DatumFactory.createBool(time.equals(((TimeDatum) datum).time));
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return time.compareTo(((TimeDatum)datum).time);
    } else if (datum instanceof NullDatum || datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException();
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof TimeDatum) {
      TimeDatum another = (TimeDatum) obj;
      return time.isEqual(another.time);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return time.hashCode();
  }

  public LocalTime getTime() {
    return time;
  }
}
