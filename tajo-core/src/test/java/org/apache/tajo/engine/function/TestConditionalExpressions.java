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
    testSimpleEval("select coalesce('value1', 'value2');", new String[]{"value1"});
    testSimpleEval("select coalesce(null, 'value2');", new String[]{"value2"});
    testSimpleEval("select coalesce(null, null, 'value3');", new String[]{"value3"});
    testSimpleEval("select coalesce('value1', null, 'value3');", new String[]{"value1"});
    testSimpleEval("select coalesce(null, 'value2', 'value3');", new String[]{"value2"});
    testSimpleEval("select coalesce('value1');", new String[]{"value1"});
    testSimpleEval("select coalesce(null);", new String[]{""});

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
    testSimpleEval("select coalesce(1, 2);", new String[]{"1"});
    testSimpleEval("select coalesce(null, 2);", new String[]{"2"});
    testSimpleEval("select coalesce(null, null, 3);", new String[]{"3"});
    testSimpleEval("select coalesce(1, null, 3);", new String[]{"1"});
    testSimpleEval("select coalesce(null, 2, 3);", new String[]{"2"});
    testSimpleEval("select coalesce(1);", new String[]{"1"});
    testSimpleEval("select coalesce(null);", new String[]{""});

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
    testSimpleEval("select coalesce(1.0, 2.0);", new String[]{"1.0"});
    testSimpleEval("select coalesce(null, 2.0);", new String[]{"2.0"});
    testSimpleEval("select coalesce(null, null, 3.0);", new String[]{"3.0"});
    testSimpleEval("select coalesce(1.0, null, 3.0);", new String[]{"1.0"});
    testSimpleEval("select coalesce(null, 2.0, 3.0);", new String[]{"2.0"});
    testSimpleEval("select coalesce(1.0);", new String[]{"1.0"});
    testSimpleEval("select coalesce(null);", new String[]{""});

    //no matched function
    try {
      testSimpleEval("select coalesce('value1', null, 3.0);", new String[]{"1.0"});
      fail("coalesce(TEXT, NULL, FLOAT8) not defined. So should throw exception.");
    } catch (NoSuchFunctionException e) {
      // success
    }

    try {
      testSimpleEval("select coalesce(null, 'value2', 3.0);", new String[]{"2.0"});
      fail("coalesce(NULL, TEXT, FLOAT8) not defined. So should throw exception.");
    } catch (NoSuchFunctionException e) {
      //success
    }
  }
}
