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
    testSimpleEval("select to_char(-12345, '999') ", new String[]{"-###"});

    testSimpleEval("select to_char(123.123, '999.999') ", new String[]{"123.123"});
    testSimpleEval("select to_char(123.325, '999.99') ", new String[]{"123.33"});
    testSimpleEval("select to_char(123.12345, '9999999.99999999999999') ", new String[]{"    123.12345000000000"});
    //testSimpleEval("select to_char(1234567890.123456789, '999999999999.9999999999999') ", new String[]{"  1234567890.1234567890000"});
    testSimpleEval("select to_char(123.325, '99999999.99') ", new String[]{"     123.33"});
    testSimpleEval("select to_char(123.325, '0999.99') ", new String[]{"0123.33"});
    testSimpleEval("select to_char(0.325, '0999.99') ", new String[]{"0000.33"});
    testSimpleEval("select to_char(-0.325, '0999.99') ", new String[]{"-0000.33"});
    testSimpleEval("select to_char(-0.325, '9999.99') ", new String[]{"   -0.33"});
    testSimpleEval("select to_char(123.325, '999RN9999.999') ", new String[]{""});
    testSimpleEval("select to_char(12345, '99D999') ", new String[]{""});
    testSimpleEval("select to_char(12345, '90999999.99999') ", new String[]{"00012345.00000"});
    testSimpleEval("select to_char(12345.12345, '99999') ", new String[]{"12345"});
    testSimpleEval("select to_char(12345.12345, '99999.9999.9999') ", new String[]{""});

    testSimpleEval("select to_char(123456, '09,999,999') ", new String[]{"00,123,456"});
    testSimpleEval("select to_char(123.12345, '0999999.999,999,9') ", new String[]{"0000123.123,450,0"});
    testSimpleEval("select to_char(123456, '99,999,999') ", new String[]{"  123,456"});
    testSimpleEval("select to_char(123.12345, '999999.999,999,9') ", new String[]{"   123.123,450,0"});

    testSimpleEval("select to_char(-123456, '09,999,999') ", new String[]{"-00,123,456"});
    testSimpleEval("select to_char(-123.12345, '0999999.999,999,9') ", new String[]{"-0000123.123,450,0"});
    testSimpleEval("select to_char(-123456, '99,999,999') ", new String[]{"  -123,456"});
    testSimpleEval("select to_char(-123.12345, '999999.999,999,9') ", new String[]{"   -123.123,450,0"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", TEXT);

    testEval(schema, "table1", ",", "select to_char(col1, col2) from table1", new String[]{""});
    testEval(schema, "table1", "0,", "select to_char(col1, col2) from table1", new String[]{""});
    testEval(schema, "table1", ",'999'", "select to_char(col1, col2) from table1", new String[]{""});
  }

  @Test
  public void testToCharJava() throws IOException {
    testSimpleEval("select to_char_java(123, '#######') ", new String[]{"123"});
    testSimpleEval("select to_char_java(-12345, '0000000') ", new String[]{"-0012345"});
    testSimpleEval("select to_char_java(12345, '#####') ", new String[]{"12345"});
    testSimpleEval("select to_char_java(12345, '###') ", new String[]{"12345"});
    testSimpleEval("select to_char_java(-12345, '###') ", new String[]{"-12345"});

    testSimpleEval("select to_char_java(123.123, '###.###') ", new String[]{"123.123"});
    testSimpleEval("select to_char_java(123.325, '###.##') ", new String[]{"123.32"});
    testSimpleEval("select to_char_java(123.12345, '#######.##############') ", new String[]{"123.12345"});
    testSimpleEval("select to_char_java(1234567890.123456789, '############.#############') ", new String[]{"1234567890.1234567"});
    testSimpleEval("select to_char_java(123.325, '########.##') ", new String[]{"123.32"});
    testSimpleEval("select to_char_java(-0.325, '####.##') ", new String[]{"-0.32"});
    testSimpleEval("select to_char_java(0.325, '0000.##') ", new String[]{"0000.32"});
    testSimpleEval("select to_char_java(12345.12345, '#####') ", new String[]{"12345"});

    testSimpleEval("select to_char_java(123456, '#,###,###') ", new String[]{"123,456"});
    testSimpleEval("select to_char_java(123456, '##,###,###') ", new String[]{"123,456"});
    testSimpleEval("select to_char_java(123456.12345, '###,###.#######') ", new String[]{"123,456.12345"});
    testSimpleEval("select to_char_java(123456.12345, '###,###.0000000') ", new String[]{"123,456.1234500"});

    testSimpleEval("select to_char_java(-123456, '##,###,###') ", new String[]{"-123,456"});

    testSimpleEval("select to_char_java(12345) ", new String[]{"12345"});
  }
}