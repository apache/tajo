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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.UndefinedFunctionException;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.apache.tajo.exception.TajoException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestConditionalExpressions extends ExprTestBase {
  @Test
  public void testCaseWhens1() throws TajoException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT1);
    schema.addColumn("col2", TajoDataTypes.Type.INT2);
    schema.addColumn("col3", TajoDataTypes.Type.INT4);
    schema.addColumn("col4", TajoDataTypes.Type.INT8);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col6", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col7", TajoDataTypes.Type.TEXT);
    schema.addColumn("col8", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));
    schema.addColumn("col9", TajoDataTypes.Type.INT4);

    testEval(schema, "table1", "1,2,3,4,5.0,6.0,text,abc,",
        "select case when col1 between 1 and 3 then 10 else 100 end from table1;",
        new String [] {"10"});
    testEval(schema, "table1", "1,2,3,4,5.0,6.0,text,abc,",
        "select case when col1 > 1 then 10 when col1 > 2 then 20 else 100 end from table1;",
        new String [] {"100"});
    testEval(schema, "table1", "1,2,3,4,5.0,6.0,text,abc,",
        "select case col1 when 1 then 10 when 2 then 20 else 100 end from table1;",
        new String [] {"10"});
    testEval(schema, "table1", "1,2,3,4,5.0,6.0,text,abc,",
        "select case col9 when 1 then 10 when 2 then 20 else 100 end is null from table1;",
        new String [] {"f"});
  }

  @Test
  public void testCaseWhensWithNullReturn() throws TajoException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.TEXT);
    schema.addColumn("col2", TajoDataTypes.Type.TEXT);

    testEval(schema, "table1", "str1,str2",
        "SELECT CASE WHEN col1 IS NOT NULL THEN col2 ELSE NULL END FROM table1",
        new String[]{"str2"});
    testEval(schema, "table1", ",str2",
        "SELECT CASE WHEN col1 IS NOT NULL THEN col2 ELSE NULL END FROM table1",
        new String[]{NullDatum.get().toString()});
  }

  @Test
  public void testCaseWhensWithCommonExpression() throws TajoException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT4);

    testEval(schema, "table1", "1,2,3",
        "SELECT CASE WHEN col1 = 1 THEN 1 WHEN col1 = 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"1"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE WHEN col2 = 1 THEN 1 WHEN col2 = 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"2"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE WHEN col3 = 1 THEN 1 WHEN col3 = 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"3"});

    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col1 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"1"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col2 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"2"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col3 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"3"});

    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col1 WHEN 1 THEN 'aaa' WHEN 2 THEN 'bbb' ELSE 'ccc' END FROM table1",
        new String [] {"aaa"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col2 WHEN 1 THEN 'aaa' WHEN 2 THEN 'bbb' ELSE 'ccc' END FROM table1",
        new String [] {"bbb"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col3 WHEN 1 THEN 'aaa' WHEN 2 THEN 'bbb' ELSE 'ccc' END FROM table1",
        new String [] {"ccc"});
  }

  @Test
  public void testCaseWhensWithCommonExpressionAndNull() throws TajoException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT4);

    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col1 WHEN 1 THEN NULL WHEN 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {NullDatum.get().toString()});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col2 WHEN 1 THEN NULL WHEN 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"2"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col3 WHEN 1 THEN NULL WHEN 2 THEN 2 ELSE 3 END FROM table1",
        new String [] {"3"});

    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col1 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE NULL END FROM table1",
        new String [] {"1"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col2 WHEN 1 THEN NULL WHEN 2 THEN 2 ELSE NULL END FROM table1",
        new String [] {"2"});
    testEval(schema, "table1", "1,2,3",
        "SELECT CASE col3 WHEN 1 THEN NULL WHEN 2 THEN 2 ELSE NULL END FROM table1",
        new String [] {NullDatum.get().toString()});
  }

  @Test
  public void testCoalesceText() throws Exception {
    testSimpleEval("select coalesce('value1', 'value2');", new String[]{"value1"});
    testSimpleEval("select coalesce(null, 'value2');", new String[]{"value2"});
    testSimpleEval("select coalesce(null, null, 'value3');", new String[]{"value3"});
    testSimpleEval("select coalesce('value1', null, 'value3');", new String[]{"value1"});
    testSimpleEval("select coalesce(null, 'value2', 'value3');", new String[]{"value2"});
    testSimpleEval("select coalesce('value1');", new String[]{"value1"});
    testSimpleEval("select coalesce(null);", new String[]{NullDatum.get().toString()});

    //no matched function
    try {
      testSimpleEval("select coalesce(null, 2, 'value3');", new String[]{"2"});
      fail("coalesce(NULL, INT, TEXT) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
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
    testSimpleEval("select coalesce(null);", new String[]{NullDatum.get().toString()});

    //no matched function
    try {
      testSimpleEval("select coalesce(null, 'value2', 3);", new String[]{"2"});
      fail("coalesce(NULL, TEXT, INT) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
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
    testSimpleEval("select coalesce(null);", new String[]{NullDatum.get().toString()});

    //no matched function
    try {
      testSimpleEval("select coalesce('value1', null, 3.0);", new String[]{"1.0"});
      fail("coalesce(TEXT, NULL, FLOAT8) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
      // success
    }

    try {
      testSimpleEval("select coalesce(null, 'value2', 3.0);", new String[]{"2.0"});
      fail("coalesce(NULL, TEXT, FLOAT8) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
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
        new String[]{"2014-01-01 00:00:00"});
    testSimpleEval("select coalesce(null, null, timestamp '2014-01-01 00:00:00');",
        new String[]{"2014-01-01 00:00:00"});
    testSimpleEval("select coalesce(timestamp '2014-01-01 00:00:00', null, timestamp '2014-01-02 00:00:00');",
        new String[]{"2014-01-01 00:00:00"});
    testSimpleEval("select coalesce(null, timestamp '2014-01-01 00:00:00', timestamp '2014-02-01 00:00:00');",
        new String[]{"2014-01-01 00:00:00"});
  }

  @Test
  public void testCoalesceTime() throws Exception {
    testSimpleEval("select coalesce(null, time '12:00:00');",
        new String[]{"12:00:00"});
    testSimpleEval("select coalesce(null, null, time '12:00:00');",
        new String[]{"12:00:00"});
    testSimpleEval("select coalesce(time '12:00:00', null, time '13:00:00');",
        new String[]{"12:00:00"});
    testSimpleEval("select coalesce(null, time '12:00:00', time '13:00:00');",
        new String[]{"12:00:00"});
  }

  @Test
  public void testCoalesceDate() throws Exception {
    testSimpleEval("select coalesce(null, date '2014-01-01');", new String[]{"2014-01-01"});
    testSimpleEval("select coalesce(null, null, date '2014-01-01');", new String[]{"2014-01-01"});
    testSimpleEval("select coalesce(date '2014-01-01', null, date '2014-02-01');", new String[]{"2014-01-01"});
    testSimpleEval("select coalesce(null, date '2014-01-01', date '2014-02-01');", new String[]{"2014-01-01"});
  }
}
