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
import static org.apache.tajo.common.TajoDataTypes.Type.INT8;

public class TestMathFunctions extends ExprTestBase {
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

  @Test
  public void testSin() throws IOException {
    testSimpleEval("select sin(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select sin(0.7) as col1 ", new String[]{"0.644217687237691"});
    testSimpleEval("select sin(1.2) as col1 ", new String[]{"0.9320390859672263"});
//    testSimpleEval("select sin(-0.5) as col1 ", new String[]{"-0.479425538604203"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select sin(col1 + col2 + col3) from table1",
        new String[]{"0.963558185417193"});
  }


  @Test
  public void testCos() throws IOException {
    testSimpleEval("select cos(0.0) as col1 ", new String[]{"1.0"});
    testSimpleEval("select cos(0.7) as col1 ", new String[]{"0.7648421872844885"});
    testSimpleEval("select cos(1.2) as col1 ", new String[]{"0.3623577544766736"});
//    testSimpleEval("select cos(-0.5) as col1 ", new String[]{"0.8775825618903728"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select cos(col1 + col2 + col3) from table1",
        new String[]{"0.26749882862458735"});
  }

  @Test
  public void testTan() throws IOException {
    testSimpleEval("select tan(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select tan(0.3) as col1 ", new String[]{"0.30933624960962325"});
    testSimpleEval("select tan(0.8) as col1 ", new String[]{"1.0296385570503641"});
//    testSimpleEval("select tan(-0.5) as col1 ", new String[]{"-0.5463024898437905"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select tan(col1 - col2 - col3) from table1",
        new String[]{"0.8422883804630795"});
  }

  @Test
  public void testAsin() throws IOException {
    testSimpleEval("select asin(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select asin(0.3) as col1 ", new String[]{"0.3046926540153975"});
    testSimpleEval("select asin(0.8) as col1 ", new String[]{"0.9272952180016123"});
//    testSimpleEval("select asin(-0.5) as col1 ", new String[]{"-0.5235987755982989"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select asin(col1 - col2 - col3) from table1",
        new String[]{"0.7753974966107532"});
  }

  @Test
  public void testAcos() throws IOException {
    testSimpleEval("select acos(0.0) as col1 ", new String[]{"1.5707963267948966"});
    testSimpleEval("select acos(0.3) as col1 ", new String[]{"1.2661036727794992"});
    testSimpleEval("select acos(0.8) as col1 ", new String[]{"0.6435011087932843"});
//    testSimpleEval("select acos(-0.5) as col1 ", new String[]{"2.0943951023931957"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select acos(col1 - col2 - col3) from table1",
        new String[]{"0.7953988301841435"});
  }

  @Test
  public void testAtan() throws IOException {
    testSimpleEval("select atan(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select atan(0.8) as col1 ", new String[]{"0.6747409422235527"});
    testSimpleEval("select atan(1.2) as col1 ", new String[]{"0.8760580505981934"});
//    testSimpleEval("select atan(-0.5) as col1 ", new String[]{"-0.4636476090008061"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select atan(col1 + col2 + col3) from table1",
        new String[]{"0.9151007005533605"});
  }

  @Test
  public void testAtan2() throws IOException {
    testSimpleEval("select atan2(0.8, 0.0) as col1 ", new String[]{"1.5707963267948966"});
    testSimpleEval("select atan2(0.8, 1.1) as col1 ", new String[]{"0.628796286415433"});
    testSimpleEval("select atan2(2.7, 0.3) as col1 ", new String[]{"1.460139105621001"});
//    testSimpleEval("select atan(-0.5, 0.3) as col1 ", new String[]{"-1.0303768265243125"});
//    testSimpleEval("select atan(-0.2, -1.3) as col1 ", new String[]{"-2.988943325194528"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select atan2(col1 + col2, col3) from table1",
        new String[]{"1.4876550949064553"});
  }

  @Test
  public void testMod() throws IOException {
    testSimpleEval("select mod(9,4) as col1 ", new String[]{"1"});
    testSimpleEval("select mod(200000000001,200000000000) as col1 ", new String[]{"1"});
    testSimpleEval("select mod(200000000000,2) as col1 ", new String[]{"0"});
    testSimpleEval("select mod(2,200000000000) as col1 ", new String[]{"2"});

    Schema schema = new Schema();
    schema.addColumn("col1", INT8);
    schema.addColumn("col2", INT8);
    schema.addColumn("col3", INT8);

    testEval(schema, "table1", "9,2,3", "select mod(col1 + col2, col3) from table1", 
        new String[]{"2"});
  }

  @Test
  public void testDiv() throws IOException {
    testSimpleEval("select div(9,4) as col1 ", new String[]{"2"});
    testSimpleEval("select div(200000000001,200000000000) as col1 ", new String[]{"1"});
    testSimpleEval("select div(200000000000,2) as col1 ", new String[]{"100000000000"});
    testSimpleEval("select div(2,200000000000) as col1 ", new String[]{"0"});

    Schema schema = new Schema();
    schema.addColumn("col1", INT8);
    schema.addColumn("col2", INT8);
    schema.addColumn("col3", INT8);

    testEval(schema, "table1", "9,2,3", "select div(col1 + col2, col3) from table1", 
        new String[]{"3"});
  }
/*
  @Test
  public void testDiv() throws IOException {
    testSimpleEval("select mod(9,4) as col1 ", new String[]{"1"});
    testSimpleEval("select mod(200000000001,200000000000) as col1 ", new String[]{"1"});
    testSimpleEval("select mod(200000000000,2) as col1 ", new String[]{"0"});
    testSimpleEval("select mod(2,200000000000) as col1 ", new String[]{"2"});

    Schema schema = new Schema();
    schema.addColumn("col1", INT8);
    schema.addColumn("col2", INT8);
    schema.addColumn("col3", INT8);

    testEval(schema, "table1", "9,2,3", "select mod(col1 + col2, col3) from table1", 
        new String[]{"2"});
  }
*/
}
