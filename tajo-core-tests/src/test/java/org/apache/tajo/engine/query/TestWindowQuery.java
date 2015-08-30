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

package org.apache.tajo.engine.query;

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
public class TestWindowQuery extends QueryTestCaseBase {

  public TestWindowQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testWindow1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow7() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow8() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowBeforeLimit() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery5() throws Exception {
    // filter push down test
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery6() throws Exception {
    // filter push down test
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithAggregation1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithAggregation2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithAggregation3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithAggregation4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithAggregation5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithAggregation6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testComplexOrderBy1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRowNumber1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRowNumber2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRowNumber3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testFirstValue1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testFirstValueTime() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("time", TajoDataTypes.Type.TIME);
    String[] data = new String[]{ "1|12:11:12", "2|10:11:13", "2|05:42:41" };
    TajoTestingCluster.createTable("firstvaluetime", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString(
          "select id, first_value(time) over ( partition by id order by time ) as time_first from firstvaluetime");
      String ascExpected = "id,time_first\n" +
          "-------------------------------\n" +
          "1,12:11:12\n" +
          "2,05:42:41\n" +
          "2,05:42:41\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE firstvaluetime PURGE");
    }
  }

  @Test
  public final void testLastValue1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLastValueTime() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("time", TajoDataTypes.Type.TIME);
    String[] data = new String[]{ "1|12:11:12", "2|10:11:13", "2|05:42:41" };
    TajoTestingCluster.createTable("lastvaluetime", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString(
          "select id, last_value(time) over ( partition by id order by time ) as time_last from lastvaluetime");
      String ascExpected = "id,time_last\n" +
          "-------------------------------\n" +
          "1,12:11:12\n" +
          "2,10:11:13\n" +
          "2,10:11:13\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE lastvaluetime PURGE");
    }
  }

  @Test
  public final void testLag1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLagTime() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("time", TajoDataTypes.Type.TIME);
    String[] data = new String[]{ "1|12:11:12", "2|10:11:13", "2|05:42:41" };
    TajoTestingCluster.createTable("lagtime", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString(
          "select id, lag(time, 1) over ( partition by id order by time ) as time_lag from lagtime");
      String ascExpected = "id,time_lag\n" +
          "-------------------------------\n" +
          "1,null\n" +
          "2,null\n" +
          "2,05:42:41\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE lagtime PURGE");
    }
  }

  @Test
  public final void testLagWithNoArgs() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLagWithDefault() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLead1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeadTime() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("time", TajoDataTypes.Type.TIME);
    String[] data = new String[]{ "1|12:11:12", "2|10:11:13", "2|05:42:41" };
    TajoTestingCluster.createTable("leadtime", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString(
          "select id, lead(time, 1) over ( partition by id order by time ) as time_lead from leadtime");
      String ascExpected = "id,time_lead\n" +
          "-------------------------------\n" +
          "1,null\n" +
          "2,10:11:13\n" +
          "2,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE leadtime PURGE");
    }
  }

  @Test
  public final void testLeadWithNoArgs() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeadWithDefault() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testStdDevSamp1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testStdDevPop1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testMultipleWindow() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("time", TajoDataTypes.Type.TIME);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1|12:11:12|abc", "2|10:11:13|def", "2|05:42:41|ghi" };
    TajoTestingCluster.createTable("multiwindow", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString(
          "select id, last_value(time) over ( partition by id order by time ) as time_last, last_value(name) over ( partition by id order by time ) as name_last from multiwindow");
      String ascExpected = "id,time_last,name_last\n" +
          "-------------------------------\n" +
          "1,12:11:12,abc\n" +
          "2,10:11:13,def\n" +
          "2,10:11:13,def\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE multiwindow PURGE");
    }
  }
}