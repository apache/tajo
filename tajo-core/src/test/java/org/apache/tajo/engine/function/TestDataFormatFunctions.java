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
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

public class TestDataFormatFunctions extends ExprTestBase {

  @Test
  public void testToNumber() throws IOException {
    testSimpleEval("select to_number('-1,2,3', '9999999') ", new String[]{String.valueOf("-123")});
    testSimpleEval("select to_number('123.123', '9999999.999') ", new String[]{String.valueOf("123.123")});
    testSimpleEval("select to_number('123.123', '9999999.99') ", new String[]{String.valueOf("123.12")});
    testSimpleEval("select to_number('123.125', '9999999.99') ", new String[]{String.valueOf("123.13")});
    testSimpleEval("select to_number('-123.125', '9999999.99') ", new String[]{String.valueOf("-123.13")});
    testSimpleEval("select to_number('123.123', '99.999') ", new String[]{String.valueOf("")});
    testSimpleEval("select to_number('123.123', '9999999') ", new String[]{String.valueOf("123")});
    testSimpleEval("select to_number('123.12345', '9999999.9999') ", new String[]{String.valueOf("123.1235")});
    testSimpleEval("select to_number('123.12345', '99999999999') ", new String[]{String.valueOf("123")});
    testSimpleEval("select to_number('123,123.123,45', '99999999999.99999') ", new String[]{String.valueOf("123123.12345")});
    testSimpleEval("select to_number('-123,123.123,45', '99999999999.99999') ", new String[]{String.valueOf("-123123.12345")});
  }
}