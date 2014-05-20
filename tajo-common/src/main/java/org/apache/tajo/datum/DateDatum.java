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
import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateDatum extends Datum {
  public static final int SIZE = 4;
  /** ISO 8601/SQL standard format - ex) 1997-12-17 */
  public static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd";
  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormat.forPattern(DEFAULT_FORMAT_STRING);
  private final LocalDate date;

  public DateDatum(int value) {
    super(TajoDataTypes.Type.DATE);
    date = decode(value);
  }

  public DateDatum(int year, int month, int day) {
    super(TajoDataTypes.Type.DATE);
    date = new LocalDate(year, month, day);
  }

  public DateDatum(String dateStr) {
    super(TajoDataTypes.Type.DATE);
    this.date = LocalDate.parse(dateStr, DEFAULT_FORMATTER);
  }

  public DateDatum(LocalDate date) {
    super(TajoDataTypes.Type.DATE);
    this.date = date;
  }

  public LocalDate getDate() {
    //LocalDate is immutable
    return date;
  }

  public DateDatum(byte [] bytes) {
    this(Bytes.toInt(bytes));
  }

  public int getCenturyOfEra() {
    return date.getCenturyOfEra();
  }

  public int getYear() {
    return date.getYear();
  }

  public int getMonthOfYear() {
    return date.getMonthOfYear();
  }

  public int getWeekyear() {
    return date.getWeekyear();
  }

  public int getWeekOfWeekyear() {
    return date.getWeekOfWeekyear();
  }

  public int getDayOfWeek() {
    return date.getDayOfWeek();
  }

  public int getDayOfMonth() {
    return date.getDayOfMonth();
  }

  public int getDayOfYear() {
    return date.getDayOfYear();
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
      case FLOAT8:
        return new DateDatum(date.plusDays(datum.asInt2()));
      case INTERVAL:
        IntervalDatum interval = (IntervalDatum)datum;
        LocalDate localDate;
        if (interval.getMonths() > 0) {
          localDate = date.plusMonths(interval.getMonths());
        } else {
          localDate = date;
        }
        return new TimestampDatum(localDate.toDateTimeAtStartOfDay().getMillis() + interval.getMilliSeconds());
      case TIME:
        return new TimestampDatum(createDateTime(date, ((TimeDatum)datum).getTime(), true));
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
      case FLOAT8:
        return new DateDatum(date.minusDays(datum.asInt2()));
      case INTERVAL:
        IntervalDatum interval = (IntervalDatum)datum;
        LocalDate localDate;
        if (interval.getMonths() > 0) {
          localDate = date.minusMonths(interval.getMonths());
        } else {
          localDate = date;
        }
        return new TimestampDatum(localDate.toDateTimeAtStartOfDay().getMillis() - interval.getMilliSeconds());
      case TIME:
        return new TimestampDatum(createDateTime(date, ((TimeDatum)datum).getTime(), false));
      case DATE:
        return new Int4Datum(Days.daysBetween(((DateDatum)datum).date, date).getDays());
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  public static DateTime createDateTime(LocalDate date, LocalTime time, boolean plus) {
    //TODO create too many temporary instance. This must be improved.
    DateTime dateTime = new DateTime(date.toDate().getTime());
    if (plus) {
      return dateTime
                .plusHours(time.getHourOfDay())
                .plusMinutes(time.getMinuteOfHour())
                .plusSeconds(time.getSecondOfMinute())
                .plusMillis(time.getMillisOfSecond());
    } else {
      return dateTime
                .minusHours(time.getHourOfDay())
                .minusMinutes(time.getMinuteOfHour())
                .minusSeconds(time.getSecondOfMinute())
                .minusMillis(time.getMillisOfSecond());
    }
  }

  @Override
  public int asInt4() {
    return encode();
  }

  private static LocalDate decode(int val) {
    int year = (val >> 16);
    int monthOfYear = (0xFFFF & val) >> 8;
    int dayOfMonth = (0x00FF & val);
    return new LocalDate(year, monthOfYear, dayOfMonth);
  }

  /**
   *   Year     MonthOfYear   DayOfMonth
   *  31-16       15-8          7 - 0
   *
   * 0xFF 0xFF    0xFF          0xFF
   */
  private int encode() {
    int instance = 0;
    instance |= (date.getYear() << 16); // 1970 ~ : 2 bytes
    instance |= (date.getMonthOfYear() << 8); // 1 - 12 : 1 byte
    instance |= (date.getDayOfMonth()); // 0 - 31 : 1 byte
    return instance;
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
    return date.toString(DEFAULT_FORMATTER);
  }

  public String toChars(String format) {
    return date.toString(format);
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
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return DatumFactory.createBool(date.equals(((DateDatum) datum).date));
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.DATE) {
      return date.compareTo(((DateDatum)datum).date);
    } else if (datum instanceof NullDatum || datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException();
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof DateDatum) {
      DateDatum another = (DateDatum) obj;
      return date.isEqual(another.date);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return date.hashCode();
  }
}
