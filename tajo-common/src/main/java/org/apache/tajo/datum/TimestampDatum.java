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
import org.apache.tajo.datum.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimestampDatum extends Datum {
  public static final int SIZE = 8;
  /** ISO 8601/SQL standard format - ex) 1997-12-17 07:37:16-08 */
  public static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";
  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormat.forPattern(DEFAULT_FORMAT_STRING);
  private DateTime dateTime;

  public TimestampDatum(int timestamp) {
    super(TajoDataTypes.Type.TIMESTAMP);
    dateTime = new DateTime((long)timestamp * 1000);
  }

  public TimestampDatum(DateTime dateTime) {
    super(TajoDataTypes.Type.TIMESTAMP);
    this.dateTime = dateTime;
  }

  TimestampDatum(byte [] bytes) {
    super(TajoDataTypes.Type.TIMESTAMP);
    this.dateTime = new DateTime(Bytes.toLong(bytes));
  }

  public TimestampDatum(String datetime) {
    super(TajoDataTypes.Type.TIMESTAMP);
    this.dateTime = DateTime.parse(datetime, DEFAULT_FORMATTER);
  }

  public int getUnixTime() {
    return (int) (dateTime.getMillis() / 1000);
  }

  public long getMillis() {
    return dateTime.getMillis();
  }

  public DateTime getDateTime() {
    return dateTime;
  }

  public int getYear() {
    return dateTime.getYear();
  }

  public int getMonthOfYear() {
    return dateTime.getMonthOfYear();
  }

  public int getDayOfWeek() {
    return dateTime.getDayOfWeek();
  }

  public int getDayOfMonth() {
    return dateTime.getDayOfMonth();
  }

  public int getHourOfDay() {
    return dateTime.getHourOfDay();
  }

  public int getMinuteOfHour() {
    return dateTime.getMinuteOfHour();
  }

  public int getSecondOfDay() {
    return dateTime.getSecondOfDay();
  }

  public int getSecondOfMinute() {
    return dateTime.getSecondOfMinute();
  }

  public int getMillisOfSecond() {
    return dateTime.getMillisOfSecond();
  }

  public String toString() {
    return asChars();
  }

  @Override
  public String asChars() {
    return dateTime.toString(DEFAULT_FORMATTER);
  }

  public String toChars(DateTimeFormatter format) {
    return dateTime.toString(format);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(dateTime.getMillis());
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return DatumFactory.createBool(dateTime.equals(((TimestampDatum) datum).dateTime));
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIMESTAMP) {
      return dateTime.compareTo(((TimestampDatum)datum).dateTime);
    } else if (datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException();
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof TimestampDatum) {
      TimestampDatum another = (TimestampDatum) obj;
      return dateTime.isEqual(another.dateTime);
    } else {
      throw new InvalidOperationException();
    }
  }
}
