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
  public void testToChar() throws IOException {
    testSimpleEval("select to_char(123, '9999999') ", new String[]{"    123"});
    testSimpleEval("select to_char(-12345, '0000099') ", new String[]{"-0012345"});
    testSimpleEval("select to_char(12345, '99999') ", new String[]{"12345"});
    testSimpleEval("select to_char(12345, '999') ", new String[]{"###"});

    testSimpleEval("select to_char(123.123, '999.999') ", new String[]{"123.123"});
    testSimpleEval("select to_char(123.325, '999.99') ", new String[]{"123.33"});
    testSimpleEval("select to_char(123.12345, '9999999.9999999') ", new String[]{"    123.1234500"});
    testSimpleEval("select to_char(123.325, '99999999.99') ", new String[]{"     123.33"});
    testSimpleEval("select to_char(123.325, '0999.99') ", new String[]{"0123.33"});
    testSimpleEval("select to_char(123.325, '999RN9999.999') ", new String[]{""});
    testSimpleEval("select to_char(12345, '99D999') ", new String[]{""});
    testSimpleEval("select to_char(12345, '90999999.99999') ", new String[]{"00012345.00000"});
    testSimpleEval("select to_char(12345.12345, '99999') ", new String[]{"12345"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", TEXT);

    testEval(schema, "table1", ",", "select to_char(col1, col2) from table1", new String[]{""});
    testEval(schema, "table1", "0,", "select to_char(col1, col2) from table1", new String[]{""});
    testEval(schema, "table1", ",'999'", "select to_char(col1, col2) from table1", new String[]{""});
  }
}