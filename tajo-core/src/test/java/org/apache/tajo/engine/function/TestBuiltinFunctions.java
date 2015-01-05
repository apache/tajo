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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select max(value) as max_value from table11");
      String ascExpected = "max_value\n" +
              "-------------------------------\n" +
              "-111\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(value) as min_value from table11");
      String ascExpected = "min_value\n" +
          "-------------------------------\n" +
          "111\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select max(name) as max_name from table11");
      String ascExpected = "max_name\n" +
          "-------------------------------\n" +
          "null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select min(name) as min_name from table11");
      String ascExpected = "min_name\n" +
          "-------------------------------\n" +
          "abc\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select avg(value_int) as avg_int, avg(value_long) as avg_long, avg(value_float) as avg_float, avg(value_double) as avg_double from table11");
      String ascExpected = "avg_int,avg_long,avg_float,avg_double\n" +
          "-------------------------------\n" +
          "1.5,-222.0,2.0,1.0\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select avg(value_int) as avg_int, avg(value_long) as avg_long, avg(value_float) as avg_float, avg(value_double) as avg_double from table11");
      String ascExpected = "avg_int,avg_long,avg_float,avg_double\n" +
          "-------------------------------\n" +
          "null,null,null,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select sum(value_int) as sum_int, sum(value_long) as sum_long, sum(value_float) as sum_float, sum(value_double) as sum_double from table11");
      String ascExpected = "sum_int,sum_long,sum_float,sum_double\n" +
          "-------------------------------\n" +
          "3,-444,4.0,2.0\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select sum(value_int) as sum_int, sum(value_long) as sum_long, sum(value_float) as sum_float, sum(value_double) as sum_double from table11");
      String ascExpected = "sum_int,sum_long,sum_float,sum_double\n" +
          "-------------------------------\n" +
          "null,null,null,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
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
}
