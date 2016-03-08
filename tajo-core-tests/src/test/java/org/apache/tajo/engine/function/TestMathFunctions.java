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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.apache.tajo.exception.TajoException;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

public class TestMathFunctions extends ExprTestBase {
  @Test
  public void testRound() throws TajoException {
    testSimpleEval("select round(5.1) as col1 ", new String[]{"5"});
    testSimpleEval("select round(5.5) as col1 ", new String[]{"6"});
    testSimpleEval("select round(5.6) as col1 ", new String[]{"6"});

    testSimpleEval("select round(-5.1) as col1 ", new String[]{"-5"});
    testSimpleEval("select round(-5.5) as col1 ", new String[]{"-6"});
    testSimpleEval("select round(-5.6) as col1 ", new String[]{"-6"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.4", "select round(col1 + col2 + col3) from table1",
        new String[]{"2"});

    Schema schema2 = new Schema();
    schema2.addColumn("col1", INT4);
    schema2.addColumn("col2", INT8);
    schema2.addColumn("col3", FLOAT4);
    schema2.addColumn("col4", FLOAT8);

    testEval(schema2, "table1", "9,9,9.5,9.5",
        "select round(col1), round (col2), round(col3), round(col4) from table1",
        new String [] {"9", "9", "10", "10"});
  }

  @Test
  public void testFloor() throws TajoException {
    testSimpleEval("select floor(5.1) as col1 ", new String[]{"5"});
    testSimpleEval("select floor(5.5) as col1 ", new String[]{"5"});
    testSimpleEval("select floor(5.6) as col1 ", new String[]{"5"});

    testSimpleEval("select floor(-5.1) as col1 ", new String[]{"-6"});
    testSimpleEval("select floor(-5.6) as col1 ", new String[]{"-6"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.4", "select floor(col1 + col2 + col3) from table1",
        new String[]{"1"});
  }

  @Test
  public void testCeil() throws TajoException {
    testSimpleEval("select ceil(5.0) as col1 ", new String[]{"5"});
    testSimpleEval("select ceil(5.1) as col1 ", new String[]{"6"});
    testSimpleEval("select ceil(5.5) as col1 ", new String[]{"6"});
    testSimpleEval("select ceil(5.6) as col1 ", new String[]{"6"});

    testSimpleEval("select ceil(-5.1) as col1 ", new String[]{"-5"});
    testSimpleEval("select ceil(-5.6) as col1 ", new String[]{"-5"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select ceil(col1 + col2 + col3) from table1",
        new String[]{"2"});
  }

  @Test
  public void testCeiling() throws TajoException {
    testSimpleEval("select ceiling(5.0) as col1 ", new String[]{"5"});
    testSimpleEval("select ceiling(5.1) as col1 ", new String[]{"6"});
    testSimpleEval("select ceiling(5.5) as col1 ", new String[]{"6"});
    testSimpleEval("select ceiling(5.6) as col1 ", new String[]{"6"});

    testSimpleEval("select ceiling(-5.1) as col1 ", new String[]{"-5"});
    testSimpleEval("select ceiling(-5.6) as col1 ", new String[]{"-5"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select ceiling(col1 + col2 + col3) from table1",
        new String[]{"2"});
  }

  @Test
  public void testSin() throws TajoException {
    testSimpleEval("select sin(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select sin(0.7) as col1 ", new String[]{"0.644217687237691"});
    testSimpleEval("select sin(1.2) as col1 ", new String[]{"0.9320390859672263"});
    testSimpleEval("select sin(-0.5) as col1 ", new String[]{"-0.479425538604203"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select sin(col1 + col2 + col3) from table1",
        new String[]{"0.963558185417193"});
  }


  @Test
  public void testCos() throws TajoException {
    testSimpleEval("select cos(0.0) as col1 ", new String[]{"1.0"});
    testSimpleEval("select cos(0.7) as col1 ", new String[]{"0.7648421872844885"});
    testSimpleEval("select cos(1.2) as col1 ", new String[]{"0.3623577544766736"});
    testSimpleEval("select cos(-0.5) as col1 ", new String[]{"0.8775825618903728"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select cos(col1 + col2 + col3) from table1",
        new String[]{"0.26749882862458735"});
  }

  @Test
  public void testTan() throws TajoException {
    testSimpleEval("select tan(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select tan(0.3) as col1 ", new String[]{"0.30933624960962325"});
    testSimpleEval("select tan(0.8) as col1 ", new String[]{"1.0296385570503641"});
    testSimpleEval("select tan(-0.5) as col1 ", new String[]{"-0.5463024898437905"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select tan(col1 - col2 - col3) from table1",
        new String[]{"0.8422883804630795"});
  }

  @Test
  public void testAsin() throws TajoException {
    testSimpleEval("select asin(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select asin(0.3) as col1 ", new String[]{"0.3046926540153975"});
    testSimpleEval("select asin(0.8) as col1 ", new String[]{"0.9272952180016123"});
    testSimpleEval("select asin(-0.5) as col1 ", new String[]{"-0.5235987755982989"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select asin(col1 - col2 - col3) from table1",
        new String[]{"0.7753974966107532"});
  }

  @Test
  public void testAcos() throws TajoException {
    testSimpleEval("select acos(0.0) as col1 ", new String[]{"1.5707963267948966"});
    testSimpleEval("select acos(0.3) as col1 ", new String[]{"1.2661036727794992"});
    testSimpleEval("select acos(0.8) as col1 ", new String[]{"0.6435011087932843"});
    testSimpleEval("select acos(-0.5) as col1 ", new String[]{"2.0943951023931957"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select acos(col1 - col2 - col3) from table1",
        new String[]{"0.7953988301841435"});
  }

  @Test
  public void testAtan() throws TajoException {
    testSimpleEval("select atan(0.0) as col1 ", new String[]{"0.0"});
    testSimpleEval("select atan(0.8) as col1 ", new String[]{"0.6747409422235527"});
    testSimpleEval("select atan(1.2) as col1 ", new String[]{"0.8760580505981934"});
    testSimpleEval("select atan(-0.5) as col1 ", new String[]{"-0.4636476090008061"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select atan(col1 + col2 + col3) from table1",
        new String[]{"0.9151007005533605"});
  }

  @Test
  public void testAtan2() throws TajoException {
    testSimpleEval("select atan2(0.8, 0.0) as col1 ", new String[]{"1.5707963267948966"});
    testSimpleEval("select atan2(0.8, 1.1) as col1 ", new String[]{"0.628796286415433"});
    testSimpleEval("select atan2(2.7, 0.3) as col1 ", new String[]{"1.460139105621001"});
    testSimpleEval("select atan2(-0.5, 0.3) as col1 ", new String[]{"-1.0303768265243125"});
    testSimpleEval("select atan2(-0.2, -1.3) as col1 ", new String[]{"-2.988943325194528"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select atan2(col1 + col2, col3) from table1",
        new String[]{"1.4876550949064553"});
  }

  @Test
  public void testMod() throws TajoException {
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
  public void testDiv() throws TajoException {
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

  @Test
  public void testSign() throws TajoException {
    testSimpleEval("select sign(2) as col1 ", new String[]{"1.0"});
    testSimpleEval("select sign(2.345) as col1 ", new String[]{"1.0"});
    testSimpleEval("select sign(0.3) as col1 ", new String[]{"1.0"});


    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT4);
    schema.addColumn("col3", FLOAT4);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select sign(col1 + col2 + col3) from table1",
        new String[]{"1.0"});


    Schema schema2 = new Schema();
    schema2.addColumn("col1", FLOAT8);
    schema2.addColumn("col2", FLOAT8);
    schema2.addColumn("col3", FLOAT8);

    testEval(schema2, "table1", "1.0, 0.2, 0.1", "select sign(col1 + col2 + col3) from table1",
        new String[]{"1.0"});
  }

  @Test
  public void testSqrt() throws TajoException {
    testSimpleEval("select sqrt(27.0) as col1 ", new String[]{"5.196152422706632"});
    testSimpleEval("select sqrt(64.0) as col1 ", new String[]{"8.0"});
    testSimpleEval("select sqrt(8.0) as col1 ", new String[]{"2.8284271247461903"});


    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT4);
    schema.addColumn("col3", FLOAT4);

    testEval(schema, "table1", "1.0, 0.2, 0.1", "select sqrt(col1 + col2 + col3) from table1",
        new String[]{"1.1401754564651765"});


    Schema schema2 = new Schema();
    schema2.addColumn("col1", FLOAT8);
    schema2.addColumn("col2", FLOAT8);
    schema2.addColumn("col3", FLOAT8);

    testEval(schema2, "table1", "1.0, 0.2, 0.1", "select sqrt(col1 + col2 + col3) from table1",
        new String[]{"1.140175425099138"});
  }

  @Test
  public void testExp() throws TajoException {
    testSimpleEval("select exp(1.0) as col1 ", new String[]{String.valueOf(Math.exp(1.0d))});
    testSimpleEval("select exp(1.1) as col1 ", new String[]{String.valueOf(Math.exp(1.1d))});
    testSimpleEval("select exp(1.2) as col1 ", new String[]{String.valueOf(Math.exp(1.2d))});


    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);

    testEval(schema, "table1", "1.123", "select exp(col1) from table1",
        new String[]{String.valueOf(Math.exp(1.123f))});

    Schema schema2 = new Schema();
    schema2.addColumn("col1", FLOAT8);

    testEval(schema2, "table1", "1.123", "select exp(col1) from table1",
        new String[]{String.valueOf(Math.exp(1.123d))});
  }


  @Test
  public void testAbs() throws TajoException {
    testSimpleEval("select abs(9) as col1 ", new String[]{"9"});
    testSimpleEval("select abs(-9) as col1 ", new String[]{"9"});
    testSimpleEval("select abs(200000000000) as col1 ", new String[]{"200000000000"});
    testSimpleEval("select abs(-200000000000) as col1 ", new String[]{"200000000000"});
    testSimpleEval("select abs(2.0) as col1 ", new String[]{"2.0"});
    testSimpleEval("select abs(-2.0) as col1 ", new String[]{"2.0"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT4);
    testEval(schema, "table1", "0.39,-0.39", "select abs(col1), abs(col2) from table1", new String[]{"0.39", "0.39"});

    Schema schema2 = new Schema();
    schema2.addColumn("col1", FLOAT8);
    schema2.addColumn("col2", FLOAT8);
    testEval(schema2, "table1", "0.033312347,-0.033312347", "select abs(col1), abs(col2) from table1",
        new String[]{"0.033312347", "0.033312347"});
  }

  @Test
  public void testCbrt() throws TajoException {
    testSimpleEval("select cbrt(27.0) as col1 ", new String[]{"3.0"});
    testSimpleEval("select cbrt(64.0) as col1 ", new String[]{"4.0"});
    testSimpleEval("select cbrt(8.0) as col1 ", new String[]{"2.0"});


    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT4);
    schema.addColumn("col3", FLOAT4);
    testEval(schema, "table1", "1.0, 0.2, 0.1", "select cbrt(col1 + col2 + col3) from table1",
        new String[]{"1.0913929030771317"});

    Schema schema2 = new Schema();
    schema2.addColumn("col1", FLOAT8);
    schema2.addColumn("col2", FLOAT8);
    schema2.addColumn("col3", FLOAT8);
    testEval(schema2, "table1", "1.0, 0.2, 0.1", "select cbrt(col1 + col2 + col3) from table1",
        new String[]{"1.091392883061106"});
  }

  @Test
  public void testDegrees() throws TajoException {
    testSimpleEval("select degrees(0.0) as col1 ", new String[]{String.valueOf(Math.toDegrees(0.0))});
    testSimpleEval("select degrees(0.8) as col1 ", new String[]{String.valueOf(Math.toDegrees(0.8))});
    testSimpleEval("select degrees(2.7) as col1 ", new String[]{String.valueOf(Math.toDegrees(2.7))});
    testSimpleEval("select degrees(-0.8) as col1 ", new String[]{String.valueOf(Math.toDegrees(-0.8))});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "0.8,2.7,-0.8", "select degrees(col1), degrees(col2), degrees(col3) from table1",
        new String[]{
            String.valueOf(Math.toDegrees((float)0.8)),
            String.valueOf(Math.toDegrees(2.7)),
            String.valueOf(Math.toDegrees(-0.8))
        });
  }

  @Test
  public void testPow() throws TajoException {
    testSimpleEval("select pow(9,3) as col1 ", new String[]{String.valueOf(Math.pow(9, 3))});
    testSimpleEval("select pow(1.0,3) as col1 ", new String[]{String.valueOf(Math.pow(1.0, 3))});
    testSimpleEval("select pow(20.1,3.1) as col1 ", new String[]{String.valueOf(Math.pow(20.1, 3.1))});
    testSimpleEval("select pow(null,3.1) as col1 ", new String[]{NullDatum.get().toString()});
    testSimpleEval("select pow(20.1,null) as col1 ", new String[]{NullDatum.get().toString()});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", INT4);
    schema.addColumn("col4", INT8);

    testEval(schema, "table1", "0.4,2.7,3,2", "select pow(col1, col2), pow(col3, col4) from table1",
        new String[]{
            String.valueOf(Math.pow((float) 0.4, 2.7)),
            String.valueOf(Math.pow(3, 2))
        });
  }

  @Test
  public void testRadians() throws TajoException {
    testSimpleEval("select radians(0.0) as col1 ", new String[]{String.valueOf(Math.toRadians(0.0))});
    testSimpleEval("select radians(0.8) as col1 ", new String[]{String.valueOf(Math.toRadians(0.8))});
    testSimpleEval("select radians(2.7) as col1 ", new String[]{String.valueOf(Math.toRadians(2.7))});
    testSimpleEval("select radians(-0.8) as col1 ", new String[]{String.valueOf(Math.toRadians(-0.8))});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT4);
    schema.addColumn("col2", FLOAT8);
    schema.addColumn("col3", FLOAT8);

    testEval(schema, "table1", "0.8,2.7,-0.8", "select radians(col1), radians(col2), radians(col3) from table1",
        new String[]{
            String.valueOf(Math.toRadians((float)0.8)),
            String.valueOf(Math.toRadians(2.7)),
            String.valueOf(Math.toRadians(-0.8))
        });
  }

  @Test
  public void testPi() throws TajoException {
    testSimpleEval("select pi() as col1 ", new String[]{String.valueOf(Math.PI)});
  }

  @Test
  public void testRoundWithSpecifiedPrecision() throws TajoException {
    // TODO - in order to make this test possible, testSimpleEval should take session variables. Now, we disable it.
    // divide zero
//    try {
//      testSimpleEval("select round(10.0/0.0,2) ", new String[]{""});
//      fail("10.0/0 should throw InvalidOperationException");
//    } catch (InvalidOperationException e) {
//      //success
//    }

    testSimpleEval("select round(42.4382,2) ", new String[]{"42.44"});
    testSimpleEval("select round(-42.4382,2) ", new String[]{"-42.44"});
    testSimpleEval("select round(-425,2) ", new String[]{"-425.0"});
    testSimpleEval("select round(425,2) ", new String[]{"425.0"});

    testSimpleEval("select round(1234567890,0) ", new String[]{"1.23456789E9"});
    testSimpleEval("select round(1234567890,1) ", new String[]{"1.23456789E9"});
    testSimpleEval("select round(1234567890,2) ", new String[]{"1.23456789E9"});

    testSimpleEval("select round(1.2345678901234567,13) ", new String[]{"1.2345678901235"});
    testSimpleEval("select round(1234567890.1234567,3) ", new String[]{"1.234567890123E9"});
    testSimpleEval("select round(1234567890.1234567,5) ", new String[]{"1.23456789012346E9"});

    Schema schema = new Schema();
    schema.addColumn("col1", FLOAT8);
    schema.addColumn("col2", INT4);

    testEval(schema, "table1", ",", "select round(col1, col2) from table1", new String[]{NullDatum.get().toString()});
  }
}
