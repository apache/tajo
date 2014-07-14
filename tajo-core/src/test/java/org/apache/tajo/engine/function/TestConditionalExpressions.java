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

import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestConditionalExpressions extends ExprTestBase {
  @Test
  public void testCoalesceText() throws Exception {
    testSimpleEval("select coalesce(null, 'value2');", new String[]{"value2"});
    testSimpleEval("select coalesce(null, null, 'value3');", new String[]{"value3"});
    testSimpleEval("select coalesce('value1', null, 'value3');", new String[]{"value1"});
    testSimpleEval("select coalesce(null, 'value2', 'value3');", new String[]{"value2"});

    //no matched function
    try {
      testSimpleEval("select coalesce(null, 2, 'value3');", new String[]{"2"});
      fail("coalesce(NULL, INT, TEXT) not defined. So should throw exception.");
    } catch (NoSuchFunctionException e) {
      //success
    }
  }

  @Test
  public void testCoalesceLong() throws Exception {
    testSimpleEval("select coalesce(null, 2);", new String[]{"2"});
    testSimpleEval("select coalesce(null, null, 3);", new String[]{"3"});
    testSimpleEval("select coalesce(1, null, 3);", new String[]{"1"});
    testSimpleEval("select coalesce(null, 2, 3);", new String[]{"2"});

    //no matched function
    try {
      testSimpleEval("select coalesce(null, 'value2', 3);", new String[]{"2"});
      fail("coalesce(NULL, TEXT, INT) not defined. So should throw exception.");
    } catch (NoSuchFunctionException e) {
      //success
    }
  }

  @Test
  public void testCoalesceDouble() throws Exception {
    testSimpleEval("select coalesce(null, 2.0);", new String[]{"2.0"});
    testSimpleEval("select coalesce(null, null, 3.0);", new String[]{"3.0"});
    testSimpleEval("select coalesce(1.0, null, 3.0);", new String[]{"1.0"});
    testSimpleEval("select coalesce(null, 2.0, 3.0);", new String[]{"2.0"});

    //no matched function
    try {
      testSimpleEval("select coalesce('value1', null, 3.0);", new String[]{"1.0"});
      fail("coalesce(TEXT, NULL, FLOAT8) not defined. So should throw exception.");
    } catch (NoSuchFunctionException e) {
      //success
    }

    try {
      testSimpleEval("select coalesce(null, 'value2', 3.0);", new String[]{"2.0"});
      fail("coalesce(NULL, TEXT, FLOAT8) not defined. So should throw exception.");
    } catch (NoSuchFunctionException e) {
      //success
    }
  }

  @Test
  public void testCoalesceBoolean() throws Exception {
    testSimpleEval("select coalesce(null, false);", new String[]{"f"});
    testSimpleEval("select coalesce(null, null, true);", new String[]{"t"});
    testSimpleEval("select coalesce(true, null, false);", new String[]{"t"});
    testSimpleEval("select coalesce(null, true, false);", new String[]{"t"});
 }

  @Test
  public void testCoalesceTimestamp() throws Exception {
    testSimpleEval("select coalesce(null, timestamp '2014-01-01 00:00:00');",
        new String[]{"2014-01-01 00:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select coalesce(null, null, timestamp '2014-01-01 00:00:00');",
        new String[]{"2014-01-01 00:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select coalesce(timestamp '2014-01-01 00:00:00', null, timestamp '2014-01-02 00:00:00');",
        new String[]{"2014-01-01 00:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select coalesce(null, timestamp '2014-01-01 00:00:00', timestamp '2014-02-01 00:00:00');",
        new String[]{"2014-01-01 00:00:00" + getUserTimeZoneDisplay()});
  }

  @Test
  public void testCoalesceTime() throws Exception {
    testSimpleEval("select coalesce(null, time '12:00:00');",
        new String[]{"12:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select coalesce(null, null, time '12:00:00');",
        new String[]{"12:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select coalesce(time '12:00:00', null, time '13:00:00');",
        new String[]{"12:00:00" + getUserTimeZoneDisplay()});
    testSimpleEval("select coalesce(null, time '12:00:00', time '13:00:00');",
        new String[]{"12:00:00" + getUserTimeZoneDisplay()});
  }

  @Test
  public void testCoalesceDate() throws Exception {
    testSimpleEval("select coalesce(null, date '2014-01-01');", new String[]{"2014-01-01"});
    testSimpleEval("select coalesce(null, null, date '2014-01-01');", new String[]{"2014-01-01"});
    testSimpleEval("select coalesce(date '2014-01-01', null, date '2014-02-01');", new String[]{"2014-01-01"});
    testSimpleEval("select coalesce(null, date '2014-01-01', date '2014-02-01');", new String[]{"2014-01-01"});
  }
}
