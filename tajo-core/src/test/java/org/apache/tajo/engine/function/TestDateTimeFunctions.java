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

package org.apache.tajo.engine.function;


import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

public class TestDateTimeFunctions extends ExprTestBase {

  @Test
  public void testToTimestamp() throws IOException {
    long expectedTimestamp = System.currentTimeMillis();
    DateTime expectedDateTime = new DateTime(expectedTimestamp);

    // (expectedTimestamp / 1000) means the translation from millis seconds to unix timestamp
    String q1 = String.format("select to_timestamp(%d);", (expectedTimestamp / 1000));
    testSimpleEval(q1, new String[]{expectedDateTime.toString(TimestampDatum.DEFAULT_FORMAT_STRING)});
  }

  @Test
  public void testToChar() throws IOException {
    long expectedTimestamp = System.currentTimeMillis();
    DateTime expectedDateTime = new DateTime(expectedTimestamp);
    String dateFormatStr = "yyyy-MM";
    // (expectedTimestamp / 1000) means the translation from millis seconds to unix timestamp
    String q = String.format("select to_char(to_timestamp(%d), 'yyyy-MM');", (expectedTimestamp / 1000));
    testSimpleEval(q, new String[]{expectedDateTime.toString(dateFormatStr)});
  }

  @Test
  public void testExtract() throws IOException {
    Schema schema2 = new Schema();
    schema2.addColumn("col1", TIMESTAMP);
    testEval(schema2, "table1",
        "1970-01-17 10:09:37",
        "select extract(year from col1), extract(month from col1), extract(day from col1) from table1;",
        new String[]{"1970.0", "1.0", "17.0"});

    Schema schema3 = new Schema();
    schema3.addColumn("col1", TIME);
    testEval(schema3, "table1",
        "10:09:37.5",
        "select extract(hour from col1), extract(minute from col1), extract(second from col1) from table1;",
        new String[]{"10.0", "9.0", "37.5"});

    Schema schema4 = new Schema();
    schema4.addColumn("col1", DATE);
    testEval(schema4, "table1",
        "1970-01-17",
        "select extract(year from col1), extract(month from col1), extract(day from col1) from table1;",
        new String[]{"1970.0", "1.0", "17.0"});

    testSimpleEval("select extract(century from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"19.0"});

    testSimpleEval("select extract(century from DATE '1970-01-17');", new String[]{"19.0"});

    testSimpleEval("select extract(decade from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"197.0"});

    testSimpleEval("select extract(decade from DATE '1970-01-17');", new String[]{"197.0"});

    testSimpleEval("select extract(millennium from TIMESTAMP '2001-02-16 10:09:37');", new String[]{"3.0"});
    testSimpleEval("select extract(millennium from TIMESTAMP '2000-02-16 10:09:37');", new String[]{"2.0"});

    testSimpleEval("select extract(millennium from DATE '2001-02-16');", new String[]{"3.0"});
    testSimpleEval("select extract(millennium from DATE '2000-02-16');", new String[]{"2.0"});

    testSimpleEval("select extract(year from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"1970.0"});
    testSimpleEval("select extract(month from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"1.0"});
    testSimpleEval("select extract(day from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"17.0"});

    testSimpleEval("select extract(hour from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"10.0"});
    testSimpleEval("select extract(minute from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"9.0"});
    testSimpleEval("select extract(second from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"37.0"});
    testSimpleEval("select extract(second from TIMESTAMP '1970-01-17 10:09:37.5');", new String[]{"37.5"});

    testSimpleEval("select extract(hour from TIME '10:09:37');", new String[]{"10.0"});
    testSimpleEval("select extract(minute from TIME '10:09:37');", new String[]{"9.0"});
    testSimpleEval("select extract(second from TIME '10:09:37');", new String[]{"37.0"});
    testSimpleEval("select extract(second from TIME '10:09:37.5');", new String[]{"37.5"});

    testSimpleEval("select extract(year from DATE '1970-01-17');", new String[]{"1970.0"});
    testSimpleEval("select extract(month from DATE '1970-01-17');", new String[]{"1.0"});
    testSimpleEval("select extract(day from DATE '1970-01-17');", new String[]{"17.0"});

    testSimpleEval("select extract(milliseconds from TIMESTAMP '1970-01-17 10:09:37.5');", new String[]{"37500.0"});
    testSimpleEval("select extract(milliseconds from TIME '10:09:37.123');", new String[]{"37123.0"});

    testSimpleEval("select extract(microseconds from TIMESTAMP '1970-01-17 10:09:37.5');", new String[]{"3.75E7"});
    testSimpleEval("select extract(microseconds from TIME '10:09:37.123');", new String[]{"3.7123E7"});

    testSimpleEval("select extract(dow from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"6.0"});
    testSimpleEval("select extract(dow from TIMESTAMP '1970-01-18 10:09:37');", new String[]{"0.0"});
    testSimpleEval("select extract(isodow from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"6.0"});
    testSimpleEval("select extract(isodow from TIMESTAMP '1970-01-18 10:09:37');", new String[]{"7.0"});

    testSimpleEval("select extract(year from TIMESTAMP '2006-01-02 10:09:37');", new String[]{"2006.0"});
    testSimpleEval("select extract(year from TIMESTAMP '2006-01-01 10:09:37');", new String[]{"2006.0"});
    testSimpleEval("select extract(isoyear from TIMESTAMP '2006-01-02 10:09:37');", new String[]{"2006.0"});
    testSimpleEval("select extract(isoyear from TIMESTAMP '2006-01-01 10:09:37');", new String[]{"2005.0"});

    testSimpleEval("select extract(quarter from TIMESTAMP '2006-02-01 10:09:37');", new String[]{"1.0"});
    testSimpleEval("select extract(quarter from TIMESTAMP '2006-04-01 10:09:37');", new String[]{"2.0"});
    testSimpleEval("select extract(quarter from TIMESTAMP '2006-07-01 10:09:37');", new String[]{"3.0"});
    testSimpleEval("select extract(quarter from TIMESTAMP '2006-12-01 10:09:37');", new String[]{"4.0"});

    testSimpleEval("select extract(week from TIMESTAMP '1970-01-17 10:09:37');", new String[]{"3.0"});

    testSimpleEval("select extract(dow from DATE '1970-01-17');", new String[]{"6.0"});
    testSimpleEval("select extract(dow from DATE '1970-01-18');", new String[]{"0.0"});
    testSimpleEval("select extract(isodow from DATE '1970-01-17');", new String[]{"6.0"});
    testSimpleEval("select extract(isodow from DATE '1970-01-18');", new String[]{"7.0"});

    testSimpleEval("select extract(year from DATE '2006-01-02');", new String[]{"2006.0"});
    testSimpleEval("select extract(year from DATE '2006-01-01');", new String[]{"2006.0"});
    testSimpleEval("select extract(isoyear from DATE '2006-01-02');", new String[]{"2006.0"});
    testSimpleEval("select extract(isoyear from DATE '2006-01-01');", new String[]{"2005.0"});

    testSimpleEval("select extract(quarter from DATE '2006-02-01');", new String[]{"1.0"});
    testSimpleEval("select extract(quarter from DATE '2006-04-01');", new String[]{"2.0"});
    testSimpleEval("select extract(quarter from DATE '2006-07-01');", new String[]{"3.0"});
    testSimpleEval("select extract(quarter from DATE '2006-12-01');", new String[]{"4.0"});

    testSimpleEval("select extract(week from DATE '1970-01-17');", new String[]{"3.0"});
  }

  @Test
  public void testDatePart() throws IOException {
    Schema schema2 = new Schema();
    schema2.addColumn("col1", TIMESTAMP);
    testEval(schema2, "table1",
        "1970-01-17 10:09:37",
        "select date_part('year', col1), date_part('month', col1), date_part('day', col1) from table1;",
        new String[]{"1970.0", "1.0", "17.0"});

    Schema schema3 = new Schema();
    schema3.addColumn("col1", TIME);
    testEval(schema3, "table1", "10:09:37.5",
        "select date_part('hour', col1), date_part('minute', col1), date_part('second', col1) from table1;",
        new String[]{"10.0", "9.0", "37.5"});

    Schema schema4 = new Schema();
    schema4.addColumn("col1", DATE);
    testEval(schema4, "table1",
        "1970-01-17",
        "select date_part('year', col1), date_part('month', col1), date_part('day', col1) from table1;",
        new String[]{"1970.0", "1.0", "17.0"});

    testSimpleEval("select date_part('century', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"19.0"});

    testSimpleEval("select date_part('century', DATE '1970-01-17');", new String[]{"19.0"});

    testSimpleEval("select date_part('decade', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"197.0"});

    testSimpleEval("select date_part('decade', DATE '1970-01-17');", new String[]{"197.0"});

    testSimpleEval("select date_part('millennium', TIMESTAMP '2001-02-16 10:09:37');", new String[]{"3.0"});
    testSimpleEval("select date_part('millennium', TIMESTAMP '2000-02-16 10:09:37');", new String[]{"2.0"});

    testSimpleEval("select date_part('millennium', DATE '2001-02-16');", new String[]{"3.0"});
    testSimpleEval("select date_part('millennium', DATE '2000-02-16');", new String[]{"2.0"});

    testSimpleEval("select date_part('year', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"1970.0"});
    testSimpleEval("select date_part('month', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"1.0"});
    testSimpleEval("select date_part('day', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"17.0"});

    testSimpleEval("select date_part('hour', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"10.0"});
    testSimpleEval("select date_part('minute', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"9.0"});
    testSimpleEval("select date_part('second', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"37.0"});
    testSimpleEval("select date_part('second', TIMESTAMP '1970-01-17 10:09:37.5');", new String[]{"37.5"});

    testSimpleEval("select date_part('hour', TIME '10:09:37');", new String[]{"10.0"});
    testSimpleEval("select date_part('minute', TIME '10:09:37');", new String[]{"9.0"});
    testSimpleEval("select date_part('second', TIME '10:09:37');", new String[]{"37.0"});
    testSimpleEval("select date_part('second', TIME '10:09:37.5');", new String[]{"37.5"});

    testSimpleEval("select date_part('year', DATE '1970-01-17');", new String[]{"1970.0"});
    testSimpleEval("select date_part('month', DATE '1970-01-17');", new String[]{"1.0"});
    testSimpleEval("select date_part('day', DATE '1970-01-17');", new String[]{"17.0"});

    testSimpleEval("select date_part('milliseconds', TIMESTAMP '1970-01-17 10:09:37.5');", new String[]{"37500.0"});
    testSimpleEval("select date_part('milliseconds', TIME '10:09:37.123');", new String[]{"37123.0"});

    testSimpleEval("select date_part('microseconds', TIMESTAMP '1970-01-17 10:09:37.5');", new String[]{"3.75E7"});
    testSimpleEval("select date_part('microseconds', TIME '10:09:37.123');", new String[]{"3.7123E7"});

    testSimpleEval("select date_part('dow', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"6.0"});
    testSimpleEval("select date_part('dow', TIMESTAMP '1970-01-18 10:09:37');", new String[]{"0.0"});
    testSimpleEval("select date_part('isodow', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"6.0"});
    testSimpleEval("select date_part('isodow', TIMESTAMP '1970-01-18 10:09:37');", new String[]{"7.0"});

    testSimpleEval("select date_part('year', TIMESTAMP '2006-01-02 10:09:37');", new String[]{"2006.0"});
    testSimpleEval("select date_part('year', TIMESTAMP '2006-01-01 10:09:37');", new String[]{"2006.0"});
    testSimpleEval("select date_part('isoyear', TIMESTAMP '2006-01-02 10:09:37');", new String[]{"2006.0"});
    testSimpleEval("select date_part('isoyear', TIMESTAMP '2006-01-01 10:09:37');", new String[]{"2005.0"});

    testSimpleEval("select date_part('quarter', TIMESTAMP '2006-02-01 10:09:37');", new String[]{"1.0"});
    testSimpleEval("select date_part('quarter', TIMESTAMP '2006-04-01 10:09:37');", new String[]{"2.0"});
    testSimpleEval("select date_part('quarter', TIMESTAMP '2006-07-01 10:09:37');", new String[]{"3.0"});
    testSimpleEval("select date_part('quarter', TIMESTAMP '2006-12-01 10:09:37');", new String[]{"4.0"});

    testSimpleEval("select date_part('week', TIMESTAMP '1970-01-17 10:09:37');", new String[]{"3.0"});

    testSimpleEval("select date_part('dow', DATE '1970-01-17');", new String[]{"6.0"});
    testSimpleEval("select date_part('dow', DATE '1970-01-18');", new String[]{"0.0"});
    testSimpleEval("select date_part('isodow', DATE '1970-01-17');", new String[]{"6.0"});
    testSimpleEval("select date_part('isodow', DATE '1970-01-18');", new String[]{"7.0"});

    testSimpleEval("select date_part('year', DATE '2006-01-02');", new String[]{"2006.0"});
    testSimpleEval("select date_part('year', DATE '2006-01-01');", new String[]{"2006.0"});
    testSimpleEval("select date_part('isoyear', DATE '2006-01-02');", new String[]{"2006.0"});
    testSimpleEval("select date_part('isoyear', DATE '2006-01-01');", new String[]{"2005.0"});

    testSimpleEval("select date_part('quarter', DATE '2006-02-01');", new String[]{"1.0"});
    testSimpleEval("select date_part('quarter', DATE '2006-04-01');", new String[]{"2.0"});
    testSimpleEval("select date_part('quarter', DATE '2006-07-01');", new String[]{"3.0"});
    testSimpleEval("select date_part('quarter', DATE '2006-12-01');", new String[]{"4.0"});

    testSimpleEval("select date_part('week', DATE '1970-01-17');", new String[]{"3.0"});
  }

  @Test
  public void testUtcUsecTo() throws IOException {
     testSimpleEval("select utc_usec_to('day' ,1274259481071200);", new String[]{1274227200000000L+""});
     testSimpleEval("select utc_usec_to('hour' ,1274259481071200);", new String[]{1274256000000000L+""});
     testSimpleEval("select utc_usec_to('month' ,1274259481071200);", new String[]{1272672000000000L+""});
     testSimpleEval("select utc_usec_to('year' ,1274259481071200);", new String[]{1262304000000000L+""});
     testSimpleEval("select utc_usec_to('week' ,1207929480000000, 2);", new String[]{1207612800000000L+""});
  }

  @Test
  public void testToDate() throws IOException {
    testSimpleEval("select to_date('2014-01-04', 'yyyy-MM-dd')", new String[]{"2014-01-04"});
    testSimpleEval("select to_date('2014-01-04', 'yyyy-MM-dd') + interval '1 day'", new String[]{"2014-01-05 00:00:00"});
  }

  @Test
  public void testAddMonths() throws Exception {
    testSimpleEval("SELECT add_months(date '2013-12-17', 2::INT2);", new String[]{"2014-02-17 00:00:00"});
    testSimpleEval("SELECT add_months(date '2013-12-17', 2::INT4);", new String[]{"2014-02-17 00:00:00"});
    testSimpleEval("SELECT add_months(date '2013-12-17', 2::INT8);", new String[]{"2014-02-17 00:00:00"});

    testSimpleEval("SELECT add_months(timestamp '2013-12-17 12:10:20', 2::INT2);", new String[]{"2014-02-17 12:10:20"});
    testSimpleEval("SELECT add_months(timestamp '2013-12-17 12:10:20', 2::INT4);", new String[]{"2014-02-17 12:10:20"});
    testSimpleEval("SELECT add_months(timestamp '2013-12-17 12:10:20', 2::INT8);", new String[]{"2014-02-17 12:10:20"});

    testSimpleEval("SELECT add_months(date '2014-02-05', -3::INT2);", new String[]{"2013-11-05 00:00:00"});
    testSimpleEval("SELECT add_months(date '2014-02-05', -3::INT4);", new String[]{"2013-11-05 00:00:00"});
    testSimpleEval("SELECT add_months(date '2014-02-05', -3::INT8);", new String[]{"2013-11-05 00:00:00"});

    testSimpleEval("SELECT add_months(timestamp '2014-02-05 12:10:20', -3::INT2);", new String[]{"2013-11-05 12:10:20"});
    testSimpleEval("SELECT add_months(timestamp '2014-02-05 12:10:20', -3::INT4);", new String[]{"2013-11-05 12:10:20"});
    testSimpleEval("SELECT add_months(timestamp '2014-02-05 12:10:20', -3::INT8);", new String[]{"2013-11-05 12:10:20"});
  }

  @Test
  public void testAddDays() throws IOException {
    testSimpleEval("SELECT add_days(date '2013-12-30', 5::INT2);", new String[]{"2014-01-04 00:00:00"});
    testSimpleEval("SELECT add_days(date '2013-12-30', 5::INT4);", new String[]{"2014-01-04 00:00:00"});
    testSimpleEval("SELECT add_days(date '2013-12-30', 5::INT8);", new String[]{"2014-01-04 00:00:00"});

    testSimpleEval("SELECT add_days(timestamp '2013-12-30 12:10:20', 5::INT2);", new String[]{"2014-01-04 12:10:20"});
    testSimpleEval("SELECT add_days(timestamp '2013-12-30 12:10:20', 5::INT4);", new String[]{"2014-01-04 12:10:20"});
    testSimpleEval("SELECT add_days(timestamp '2013-12-30 12:10:20', 5::INT8);", new String[]{"2014-01-04 12:10:20"});

    testSimpleEval("SELECT add_days(date '2013-12-05', -7::INT2);", new String[]{"2013-11-28 00:00:00"});
    testSimpleEval("SELECT add_days(date '2013-12-05', -7::INT4);", new String[]{"2013-11-28 00:00:00"});
    testSimpleEval("SELECT add_days(date '2013-12-05', -7::INT8);", new String[]{"2013-11-28 00:00:00"});

    testSimpleEval("SELECT add_days(timestamp '2013-12-05 12:10:20', -7::INT2);", new String[]{"2013-11-28 12:10:20"});
    testSimpleEval("SELECT add_days(timestamp '2013-12-05 12:10:20', -7::INT4);", new String[]{"2013-11-28 12:10:20"});
    testSimpleEval("SELECT add_days(timestamp '2013-12-05 12:10:20', -7::INT8);", new String[]{"2013-11-28 12:10:20"});
  }
}
