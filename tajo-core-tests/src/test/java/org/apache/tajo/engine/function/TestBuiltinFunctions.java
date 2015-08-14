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

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestBuiltinFunctions extends QueryTestCaseBase {

  public TestBuiltinFunctions() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testMaxLong() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testMaxLongWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value", TajoDataTypes.Type.INT8);
    String[] data = new String[]{ "1|-111", "2|\\N", "3|-333" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select max(value) as max_value from testbuiltin11");
      String ascExpected = "max_value\n" +
              "-------------------------------\n" +
              "-111\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testMinMaxDate() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("value", TajoDataTypes.Type.DATE);
    String[] data = new String[]{ "2014-01-02", "2014-12-01", "2015-01-01", "1999-08-09", "2000-03-01" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value, max(value) as max_value from testbuiltin11");
      String ascExpected = "min_value,max_value\n" +
              "-------------------------------\n" +
              "1999-08-09,2015-01-01\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testMinMaxDateWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("value", TajoDataTypes.Type.DATE);
    String[] data = new String[]{ "2014-01-02", "2014-12-01", "\\N", "\\N", "2000-03-01" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value, max(value) as max_value from testbuiltin11");
      String ascExpected = "min_value,max_value\n" +
              "-------------------------------\n" +
              "2000-03-01,2014-12-01\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testMinMaxTime() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("value", TajoDataTypes.Type.TIME);
    String[] data = new String[]{ "11:11:11", "23:12:50", "00:00:01", "09:59:59", "12:13:14" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value, max(value) as max_value from testbuiltin11");
      String ascExpected = "min_value,max_value\n" +
              "-------------------------------\n" +
              "00:00:01,23:12:50\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testMinMaxTimeWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("value", TajoDataTypes.Type.TIME);
    String[] data = new String[]{ "11:11:11", "\\N", "\\N", "09:59:59", "12:13:14" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value, max(value) as max_value from testbuiltin11");
      String ascExpected = "min_value,max_value\n" +
              "-------------------------------\n" +
              "09:59:59,12:13:14\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testMinMaxTimestamp() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("value", TajoDataTypes.Type.TIMESTAMP);
    String[] data = new String[]{ "1999-01-01 11:11:11", "2015-01-01 23:12:50", "2016-12-24 00:00:01", 
            "1977-05-04 09:59:59", "2002-11-21 12:13:14" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value, max(value) as max_value from testbuiltin11");
      String ascExpected = "min_value,max_value\n" +
              "-------------------------------\n" +
              "1977-05-04 09:59:59,2016-12-24 00:00:01\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testMinMaxTimestampWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("value", TajoDataTypes.Type.TIMESTAMP);
    String[] data = new String[]{ "1999-01-01 11:11:11", "2015-01-01 23:12:50", "\\N",
            "\\N", "2002-11-21 12:13:14" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value, max(value) as max_value from testbuiltin11");
      String ascExpected = "min_value,max_value\n" +
              "-------------------------------\n" +
              "1999-01-01 11:11:11,2015-01-01 23:12:50\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testMinLong() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testMinLongWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value", TajoDataTypes.Type.INT8);
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value from testbuiltin11");
      String ascExpected = "min_value\n" +
          "-------------------------------\n" +
          "111\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testMaxString() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testMaxStringWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1|\\N", "2|\\N", "3|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select max(name) as max_name from testbuiltin11");
      String ascExpected = "max_name\n" +
          "-------------------------------\n" +
          "null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testMinString() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testMinStringWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1|def", "2|\\N", "3|abc" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(name) as min_name from testbuiltin11");
      String ascExpected = "min_name\n" +
          "-------------------------------\n" +
          "abc\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testCount() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testAvgDouble() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testAvgLong() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testAvgInt() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testAvgLongOverflow() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testAvgWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{ "1|\\N|-111|1.2|-50.5", "2|1|\\N|\\N|52.5", "3|2|-333|2.8|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select avg(value_int) as avg_int, avg(value_long) as avg_long, avg(value_float) as avg_float, avg(value_double) as avg_double from testbuiltin11");
      String ascExpected = "avg_int,avg_long,avg_float,avg_double\n" +
          "-------------------------------\n" +
          "1.5,-222.0,2.0,1.0\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testAvgWithAllNulls() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{ "1|\\N|\\N|\\N|\\N", "2|\\N|\\N|\\N|\\N", "3|\\N|\\N|\\N|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select avg(value_int) as avg_int, avg(value_long) as avg_long, avg(value_float) as avg_float, avg(value_double) as avg_double from testbuiltin11");
      String ascExpected = "avg_int,avg_long,avg_float,avg_double\n" +
          "-------------------------------\n" +
          "null,null,null,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testSumWithNull() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{ "1|\\N|-111|1.2|-50.5", "2|1|\\N|\\N|52.5", "3|2|-333|2.8|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select sum(value_int) as sum_int, sum(value_long) as sum_long, sum(value_float) as sum_float, sum(value_double) as sum_double from testbuiltin11");
      String ascExpected = "sum_int,sum_long,sum_float,sum_double\n" +
          "-------------------------------\n" +
          "3,-444,4.0,2.0\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testSumWithAllNulls() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{ "1|\\N|\\N|\\N|\\N", "2|\\N|\\N|\\N|\\N", "3|\\N|\\N|\\N|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select sum(value_int) as sum_int, sum(value_long) as sum_long, sum(value_float) as sum_float, sum(value_double) as sum_double from testbuiltin11");
      String ascExpected = "sum_int,sum_long,sum_float,sum_double\n" +
          "-------------------------------\n" +
          "null,null,null,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testStdDevSamp() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
        "1|\\N|-111|1.2|-50.5",
        "2|1|\\N|\\N|52.5",
        "3|2|-333|2.8|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select stddev_samp(value_int) as sdsamp_int, stddev_samp(value_long) as sdsamp_long, stddev_samp(value_float) as sdsamp_float, stddev_samp(value_double) as sdsamp_double from testbuiltin11");
      String ascExpected = "sdsamp_int,sdsamp_long,sdsamp_float,sdsamp_double\n" +
          "-------------------------------\n" +
          "0.7071067811865476,156.97770542341354,1.1313707824635184,72.8319984622144\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testStdDevSampWithFewNumbers() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
        "1|\\N|\\N|\\N|-50.5",
        "2|1|\\N|\\N|\\N",
        "3|\\N|\\N|\\N|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select stddev_samp(value_int) as sdsamp_int, stddev_samp(value_long) as sdsamp_long, stddev_samp(value_float) as sdsamp_float, stddev_samp(value_double) as sdsamp_double from testbuiltin11");
      String ascExpected = "sdsamp_int,sdsamp_long,sdsamp_float,sdsamp_double\n" +
          "-------------------------------\n" +
          "null,null,null,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testStdDevPop() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
        "1|\\N|-111|1.2|-50.5",
        "2|1|\\N|\\N|52.5",
        "3|2|-333|2.8|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select stddev_pop(value_int) as sdpop_int, stddev_pop(value_long) as sdpop_long, stddev_pop(value_float) as sdpop_float, stddev_pop(value_double) as sdpop_double from testbuiltin11");
      String ascExpected = "sdpop_int,sdpop_long,sdpop_float,sdpop_double\n" +
          "-------------------------------\n" +
          "0.5,111.0,0.7999999523162842,51.5\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testStdDevPopWithFewNumbers() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
        "1|\\N|\\N|\\N|-50.5",
        "2|1|\\N|\\N|\\N",
        "3|\\N|\\N|\\N|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select stddev_pop(value_int) as sdpop_int, stddev_pop(value_long) as sdpop_long, stddev_pop(value_float) as sdpop_float, stddev_pop(value_double) as sdpop_double from testbuiltin11");
      String ascExpected = "sdpop_int,sdpop_long,sdpop_float,sdpop_double\n" +
          "-------------------------------\n" +
          "0.0,null,null,0.0\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }

  }

  @Test
  public void testVarSamp() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
            "1|\\N|-111|1.2|-50.5",
            "2|1|\\N|\\N|52.5",
            "3|2|-333|2.8|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select var_samp(value_int) as vs_int, var_samp(value_long) as vs_long, var_samp(value_float) as vs_float, var_samp(value_double) as vs_double from testbuiltin11");
      String ascExpected = "vs_int,vs_long,vs_float,vs_double\n" +
              "-------------------------------\n" +
              "0.5,24642.0,1.279999847412114,5304.5\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testVarSampWithFewNumbers() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
            "1|\\N|\\N|\\N|-50.5",
            "2|1|\\N|\\N|\\N",
            "3|\\N|\\N|\\N|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select var_samp(value_int) as vsamp_int, var_samp(value_long) as vsamp_long, var_samp(value_float) as vsamp_float, var_samp(value_double) as vsamp_double from testbuiltin11");
      String ascExpected = "vsamp_int,vsamp_long,vsamp_float,vsamp_double\n" +
              "-------------------------------\n" +
              "null,null,null,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testVarPop() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
            "1|\\N|-111|1.2|-50.5",
            "2|1|\\N|\\N|52.5",
            "3|2|-333|2.8|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select var_pop(value_int) as vpop_int, var_pop(value_long) as vpop_long, var_pop(value_float) as vpop_float, var_pop(value_double) as vpop_double from testbuiltin11");
      String ascExpected = "vpop_int,vpop_long,vpop_float,vpop_double\n" +
              "-------------------------------\n" +
              "0.25,12321.0,0.639999923706057,2652.25\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

  @Test
  public void testVarPopWithFewNumbers() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
            "1|\\N|\\N|\\N|-50.5",
            "2|1|\\N|\\N|\\N",
            "3|\\N|\\N|\\N|\\N" };
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select var_pop(value_int) as vpop_int, var_pop(value_long) as vpop_long, var_pop(value_float) as vpop_float, var_pop(value_double) as vpop_double from testbuiltin11");
      String ascExpected = "vpop_int,vpop_long,vpop_float,vpop_double\n" +
              "-------------------------------\n" +
              "0.0,null,null,0.0\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }

//  @Test
//  public void testRandom() throws Exception {
//    ResultSet res = executeQuery();
//    while(res.next()) {
//      assertTrue(res.getInt(2) >= 0 && res.getInt(2) < 3);
//    }
//    cleanupQuery(res);
//  }

  @Test
  public void testSplitPart() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSplitPartByString() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSplitPartNested() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
  
  @Test
  public void testRankWithTwoTables() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");
    
    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    String[] data = new String[] {"1", "3", "2", "4"};
    TajoTestingCluster.createTable("rank_table1", schema, tableOptions, data, 1);
    schema = new Schema();
    schema.addColumn("refid", TajoDataTypes.Type.INT4);
    schema.addColumn("value", TajoDataTypes.Type.TEXT);
    data = new String[] {"1|efgh", "2|abcd", "4|erjk", "8|dfef"};
    TajoTestingCluster.createTable("rank_table2", schema, tableOptions, data, 1);
    ResultSet res = null;
    
    try {
      res = executeString("select rank() over (order by id) from rank_table1 a, rank_table2 b "
          + " where a.id = b.refid");
      String expectedString = "?windowfunction\n" +
          "-------------------------------\n" +
          "1\n" +
          "2\n" +
          "3\n";
      
      assertEquals(expectedString, resultSetToString(res));
    } finally {
      if (res != null) {
        try {
        res.close();
        } catch(Throwable ignored) {}
      }
      executeString("DROP TABLE rank_table1 PURGE");
      executeString("DROP TABLE rank_table2 PURGE");
    }
  }

  @Test
  public void testCorr() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("value_int", TajoDataTypes.Type.INT4);
    schema.addColumn("value_long", TajoDataTypes.Type.INT8);
    schema.addColumn("value_float", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("value_double", TajoDataTypes.Type.FLOAT8);
    String[] data = new String[]{
        "1|\\N|-111|1.2|-50.5",
        "2|1|\\N|\\N|52.5",
        "3|2|-333|2.8|\\N",
        "4|3|-555|2.8|43.2",
        "5|4|-111|1.1|10.2",};
    TajoTestingCluster.createTable("testbuiltin11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select corr(value_int, value_long) as corr1, corr(value_long, value_float) as corr2, corr(value_float, value_double) as corr3, corr(value_double, value_int) as corr4 from testbuiltin11");
      String ascExpected = "corr1,corr2,corr3,corr4\n" +
          "-------------------------------\n" +
          "0.5,-0.9037045658322675,0.7350290063698216,-0.8761489936497805\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testbuiltin11 PURGE");
    }
  }
}
