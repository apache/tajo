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

package org.apache.tajo.engine.eval;

import org.apache.tajo.exception.InvalidOperationException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

public class TestIntervalType extends ExprTestBase {
  @Test
  public void testIntervalPostgresqlCase() throws IOException {

    // http://www.postgresql.org/docs/8.2/static/functions-datetime.html
    testSimpleEval("select date '2001-09-28' + 7", new String[]{"2001-10-05"});
    testSimpleEval("select date '2001-09-28' + interval '1 hour'",
        new String[]{"2001-09-28 01:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select date '2001-09-28' + time '03:00'",
        new String[]{"2001-09-28 03:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select time '03:00' + date '2001-09-28'",
        new String[]{"2001-09-28 03:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select interval '1 day' + interval '1 hour'", new String[]{"1 day 01:00:00"});

    testSimpleEval("select timestamp '2001-09-28 01:00' + interval '23 hours'",
        new String[]{"2001-09-29 00:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select time '01:00' + interval '3 hours'", new String[]{"04:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select date '2001-10-01' - date '2001-09-28'", new String[]{"3"});
    testSimpleEval("select date '2001-10-01' - 7", new String[]{"2001-09-24"});
    testSimpleEval("select date '2001-09-28' - interval '1 hour'",
        new String[]{"2001-09-27 23:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select time '05:00' - time '03:00'", new String[]{"02:00:00"});
    testSimpleEval("select time '05:00' - interval '2 hours'", new String[]{"03:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select timestamp '2001-09-28 23:00' - interval '23 hours'",
        new String[]{"2001-09-28 00:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select interval '1 day' - interval '1 hour'", new String[]{"23:00:00"});

    testSimpleEval("select timestamp '2001-09-29 03:00' - timestamp '2001-09-27 12:00'", new String[]{"1 day 15:00:00"});
    testSimpleEval("select 900 * interval '1 second'", new String[]{"00:15:00"});
    testSimpleEval("select 21 * interval '1 day'", new String[]{"21 days"});
    testSimpleEval("select 3.5 * interval '1 hour'", new String[]{"03:30:00"});
    testSimpleEval("select interval '1 hour' / 1.5", new String[]{"00:40:00"});
  }

  @Test
  public void testCaseByCase() throws Exception {
    testSimpleEval("select date '2001-08-28' + interval '10 day 1 hour'",
        new String[]{"2001-09-07 01:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select interval '10 day 01:00:00' + date '2001-08-28'",
        new String[]{"2001-09-07 01:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select time '10:20:30' + interval '1 day 01:00:00'",
        new String[]{"11:20:30" + getUserTimeZoneDisplay()});
    testSimpleEval("select interval '1 day 01:00:00' + time '10:20:30'",
        new String[]{"11:20:30" + getUserTimeZoneDisplay()});
    testSimpleEval("select time '10:20:30' - interval '1 day 01:00:00'",
        new String[]{"09:20:30" + getUserTimeZoneDisplay()});

    testSimpleEval("select (interval '1 month 20 day' + interval '50 day')", new String[]{"1 month 70 days"});
    testSimpleEval("select date '2013-01-01' + interval '1 month 70 day'",
        new String[]{"2013-04-12 00:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select date '2013-01-01' + (interval '1 month 20 day' + interval '50 day')",
        new String[]{"2013-04-12 00:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select interval '1 month 70 day' + date '2013-01-01'",
        new String[]{"2013-04-12 00:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select date '2013-01-01' - interval '1 month 70 day'",
        new String[]{"2012-09-22 00:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select timestamp '2001-09-28 23:00' - interval '1 month 2 day 10:20:30'",
        new String[]{"2001-08-26 12:39:30" + getUserTimeZoneDisplay()});
    testSimpleEval("select timestamp '2001-09-28 23:00' + interval '1 month 2 day 10:20:30'",
        new String[]{"2001-10-31 09:20:30" + getUserTimeZoneDisplay()});
    testSimpleEval("select interval '1 month 2 day 10:20:30' + timestamp '2001-09-28 23:00'",
        new String[]{"2001-10-31 09:20:30" + getUserTimeZoneDisplay()});


    testSimpleEval("select interval '5 month' / 3", new String[]{"1 month 20 days"});

    // Notice: Different from postgresql result(13 days 01:02:36.4992) because of double type precision.
    testSimpleEval("select interval '1 month' / 2.3", new String[]{"13 days 01:02:36.522"});

    testSimpleEval("select interval '1 month' * 2.3", new String[]{"2 months 9 days"});
    testSimpleEval("select interval '3 year 5 month 1 hour' / 1.5", new String[]{"2 years 3 months 10 days 00:40:00"});

    testSimpleEval("select date '2001-09-28' - time '03:00'",
        new String[]{"2001-09-27 21:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select date '2014-03-20' + interval '1 day'",
        new String[]{"2014-03-21 00:00:00" + getUserTimeZoneDisplay()});

    testSimpleEval("select date '2014-03-20' - interval '1 day'",
        new String[]{"2014-03-19 00:00:00" + getUserTimeZoneDisplay()});
  }

  @Test
  public void testWrongFormatLiteral() throws Exception {
    try {
      testSimpleEval("select interval '1 month 2 day 23 hours 10:20:30'", new String[]{"DUMMY"});
      fail("hour and time format can not be used at the same time  in interval.");
    } catch (InvalidOperationException e) {
      //success
    }
  }
}
