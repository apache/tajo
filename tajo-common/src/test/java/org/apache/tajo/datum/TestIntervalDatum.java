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
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestIntervalDatum {
  @Test
  public final void parseIntervalDatum() {
    IntervalDatum datum = new IntervalDatum("3y 5month 10day 23:50:40.200");
    assertEquals("3 years 5 months 10 days 23:50:40.200", datum.asChars());

    datum = new IntervalDatum("23:50:40.200 5month 10day 3y");
    assertEquals("3 years 5 months 10 days 23:50:40.200", datum.asChars());

    datum = new IntervalDatum("3y   5month 10  day   23:50:40.200");
    assertEquals("3 years 5 months 10 days 23:50:40.200", datum.asChars());

    datum = new IntervalDatum("3years   5months 10  day   23:50:40.200");
    assertEquals("3 years 5 months 10 days 23:50:40.200", datum.asChars());

    datum = new IntervalDatum("5 hour");
    assertEquals("05:00:00", datum.asChars());
    try {
      datum = new IntervalDatum("3years   5months 10  day  1h 23:50:40.200");
      fail("hour and time format can not be used at the same time  in interval.");
    } catch (InvalidOperationException e) {
      //success
    }
  }

  @Test
  public final void testAsText() {
    IntervalDatum datum = new IntervalDatum(14,
        IntervalDatum.DAY_MILLIS + 10 * IntervalDatum.HOUR_MILLIS + 20 * IntervalDatum.MINUTE_MILLIS + 30 * 1000 + 400);
    assertEquals("1 year 2 months 1 day 10:20:30.400", datum.asChars());
  }

  @Test
  public final void testOperation() throws Exception {
    // http://www.postgresql.org/docs/8.2/static/functions-datetime.html

    // date '2001-09-28' + integer '7'	==> date '2001-10-05'
    Datum datum = DatumFactory.createDate(2001, 9, 28);
    Datum[] datums = new Datum[]{new Int2Datum((short) 7), new Int4Datum(7), new Int8Datum(7),
          new Float4Datum(7.0f), new Float8Datum(7.0f)};

    for (int i = 0; i < datums.length; i++) {
      Datum result = datum.plus(datums[i]);
      assertEquals(TajoDataTypes.Type.DATE, result.type());
      assertEquals("date '2001-09-28' + " + datums[i].asChars() + "(" + i + " th test)", "2001-10-05", result.asChars());
    }

    //TimestampDatum and TimeDatum should be TimeZone when convert to string
    // date '2001-09-28' + interval '1 hour'	==> timestamp '2001-09-28 01:00:00'
    datum = DatumFactory.createDate(2001, 9, 28);
    Datum result = datum.plus(new IntervalDatum(60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-28 01:00:00", result.asChars());

    // interval '1 hour' +  date '2001-09-28'	==> timestamp '2001-09-28 01:00:00'
    datum = new IntervalDatum(60 * 60 * 1000);
    result = datum.plus(DatumFactory.createDate(2001, 9, 28));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-28 01:00:00", result.asChars());

    // date '2001-09-28' + time '03:00' ==> timestamp '2001-09-28 03:00:00'
    datum = DatumFactory.createDate(2001, 9, 28);
    TimeDatum time = new TimeDatum(DateTimeUtil.toTime(3, 0, 0, 0));
    result = datum.plus(time);
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-28 03:00:00", result.asChars());

    // interval '1 day' + interval '1 hour'	interval '1 day 01:00:00'
    datum = new IntervalDatum(IntervalDatum.DAY_MILLIS);
    result = datum.plus(new IntervalDatum(0, 1 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.INTERVAL, result.type());
    assertEquals("1 day 01:00:00", result.asChars());

    // timestamp '2001-09-28 01:00' + interval '23 hours'	==> timestamp '2001-09-29 00:00:00'
    datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(2001, 9, 28, 1, 0, 0, 0));
    result = datum.plus(new IntervalDatum(23 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-29 00:00:00", result.asChars());

    // time '01:00' + interval '3 hours' ==> time '04:00:00'
    datum = new TimeDatum(DateTimeUtil.toTime(1, 0, 0, 0));
    result = datum.plus(new IntervalDatum(3 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIME, result.type());
    assertEquals(new TimeDatum(DateTimeUtil.toTime(4, 0, 0, 0)), result);

    // - interval '23 hours' ==> interval '-23:00:00'
    // TODO Currently Interval's inverseSign() not supported

    // date '2001-10-01' - date '2001-09-28' ==>	integer '3'
    datum = DatumFactory.createDate(2001, 10, 1);
    result = datum.minus(DatumFactory.createDate(2001, 9, 28));
    assertEquals(TajoDataTypes.Type.INT4, result.type());
    assertEquals(new Int4Datum(3), result);

    // date '2001-10-01' - integer '7' ==>	date '2001-09-24'
    datum = DatumFactory.createDate(2001, 10, 1);
    for (Datum eachDatum : datums) {
      Datum result2 = datum.minus(eachDatum);
      assertEquals(TajoDataTypes.Type.DATE, result2.type());
      assertEquals(DatumFactory.createDate(2001, 9, 24), result2);
    }

    // date '2001-09-28' - interval '1 hour' ==> timestamp '2001-09-27 23:00:00'
    datum = DatumFactory.createDate(2001, 9, 28);
    result = datum.minus(new IntervalDatum(1 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-27 23:00:00", result.asChars());

    // date '2001-09-28' - interval '1 day 1 hour' ==> timestamp '2001-09-26 23:00:00'
    // In this case all datums are UTC
    datum = DatumFactory.createDate(2001, 9, 28);
    result = datum.minus(new IntervalDatum(IntervalDatum.DAY_MILLIS + 1 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-26 23:00:00",  result.asChars());

    // time '05:00' - time '03:00' ==>	interval '02:00:00'
    datum = new TimeDatum(DateTimeUtil.toTime(5, 0, 0, 0));
    result = datum.minus(new TimeDatum(DateTimeUtil.toTime(3, 0, 0, 0)));
    assertEquals(TajoDataTypes.Type.INTERVAL, result.type());
    assertEquals(new IntervalDatum(2 * 60 * 60 * 1000), result);

    // time '05:00' - interval '2 hours' ==>	time '03:00:00'
    datum = new TimeDatum(DateTimeUtil.toTime(5, 0, 0, 0));
    result = datum.minus(new IntervalDatum(2 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIME, result.type());
    assertEquals(new TimeDatum(DateTimeUtil.toTime(3, 0, 0, 0)), result);

    // timestamp '2001-09-28 23:00' - interval '23 hours' ==>	timestamp '2001-09-28 00:00:00'
    // In this case all datums are UTC
    datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(2001, 9, 28, 23, 0, 0, 0));
    result = datum.minus(new IntervalDatum(23 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-28 00:00:00", result.asChars());

    // interval '1 day' - interval '1 hour'	==> interval '1 day -01:00:00'
    datum = new IntervalDatum(IntervalDatum.DAY_MILLIS);
    result = datum.minus(new IntervalDatum(1 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.INTERVAL, result.type());
    assertEquals(new IntervalDatum(23 * 60 * 60 * 1000), result);

    // timestamp '2001-09-29 03:00' - timestamp '2001-09-27 12:00' ==>	interval '1 day 15:00:00'
    datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(2001, 9, 29, 3, 0, 0, 0));
    result = datum.minus(new TimestampDatum(DateTimeUtil.toJulianTimestamp(2001, 9, 27, 12, 0, 0, 0)));
    assertEquals(TajoDataTypes.Type.INTERVAL, result.type());
    assertEquals(new IntervalDatum(IntervalDatum.DAY_MILLIS + 15 * 60 * 60 * 1000), result);

    // 900 * interval '1 second' ==> interval '00:15:00'
    Datum[] datum900 = new Datum[]{new Int2Datum((short)900), new Int4Datum(900), new Int8Datum(900),
        new Float4Datum(900.0f), new Float8Datum(900.0f)};

    datum = new IntervalDatum(1000);
    for (Datum aDatum900 : datum900) {
      Datum result2 = datum.multiply(aDatum900);
      assertEquals(TajoDataTypes.Type.INTERVAL, result2.type());
      assertEquals(new IntervalDatum(15 * 60 * 1000), result2);

      result2 = aDatum900.multiply(datum);
      assertEquals(TajoDataTypes.Type.INTERVAL, result2.type());
      assertEquals(new IntervalDatum(15 * 60 * 1000), result2);
    }

    // double precision '3.5' * interval '1 hour'	==> interval '03:30:00'
    datum = new Float8Datum(3.5f);
    result = datum.multiply(new IntervalDatum(1 * 60 * 60 * 1000));
    assertEquals(TajoDataTypes.Type.INTERVAL, result.type());
    assertEquals(new IntervalDatum(3 * 60 * 60 * 1000 + 30 * 60 * 1000), result);

    // interval '1 hour' / double precision '1.5' ==>	interval '00:40:00'
    datum = new IntervalDatum(1 * 60 * 60 * 1000);
    result = datum.divide(new Float8Datum(1.5f));
    assertEquals(TajoDataTypes.Type.INTERVAL, result.type());
    assertEquals(new IntervalDatum(40 * 60 * 1000), result);

    // timestamp '2001-08-31 01:00:00' + interval '1 mons' ==> timestamp 2001-09-30 01:00:00
    // In this case all datums are UTC
    datum = new TimestampDatum(DateTimeUtil.toJulianTimestamp(2001, 8, 31, 1, 0, 0, 0));
    result = datum.plus(new IntervalDatum(1, 0));
    assertEquals(TajoDataTypes.Type.TIMESTAMP, result.type());
    assertEquals("2001-09-30 01:00:00", result.asChars());
  }
}
