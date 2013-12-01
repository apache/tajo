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

import static org.apache.tajo.common.TajoDataTypes.Type.FLOAT8;

public class TestMethFunction extends ExprTestBase {
  @Test
  public void testRound() throws IOException {
    testSimpleEval("select round(5.1) as col1 ", new String[]{"5"});
    testSimpleEval("select round(5.5) as col1 ", new String[]{"6"});
    testSimpleEval("select round(5.6) as col1 ", new String[]{"6"});

//    testSimpleEval("select round(-5.1) as col1 ", new String[]{"-5"});
//    testSimpleEval("select round(-5.5) as col1 ", new String[]{"-6"});
//    testSimpleEval("select round(-5.6) as col1 ", new String[]{"-6"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.4", "select round(col1 + col2 + col3) from table1",
        new String[]{"2"});
  }

  @Test
  public void testFloor() throws IOException {
    testSimpleEval("select floor(5.1) as col1 ", new String[]{"5"});
    testSimpleEval("select floor(5.5) as col1 ", new String[]{"5"});
    testSimpleEval("select floor(5.6) as col1 ", new String[]{"5"});
//    testSimpleEval("select floor(-5.1) as col1 ", new String[]{"-6"});
//    testSimpleEval("select floor(-5.6) as col1 ", new String[]{"-6"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.4", "select floor(col1 + col2 + col3) from table1",
        new String[]{"1"});
  }

  @Test
  public void testCeil() throws IOException {
    testSimpleEval("select ceil(5.0) as col1 ", new String[]{"5"});
    testSimpleEval("select ceil(5.1) as col1 ", new String[]{"6"});
    testSimpleEval("select ceil(5.5) as col1 ", new String[]{"6"});
    testSimpleEval("select ceil(5.6) as col1 ", new String[]{"6"});
//    testSimpleEval("select ceil(-5.1) as col1 ", new String[]{"-5"});
//    testSimpleEval("select ceil(-5.6) as col1 ", new String[]{"-5"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select ceil(col1 + col2 + col3) from table1",
        new String[]{"2"});
  }
}
