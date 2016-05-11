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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * This is the unit test for null values. This test needs specialized data sets.
 * So, We separated it from other unit tests using TPC-H data set.
 */
@Category(IntegrationTest.class)
public class TestNullValues {

  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    client =  TpchTestBase.getInstance().getTestingCluster().newTajoClient();
  }

  @AfterClass
  public static void tearDown() {
    client.close();
  }

  @Test
  public final void testIsNull() throws Exception {
    String [] table = new String[] {"nulltable1"};
    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.INT4)
        .add("col2", Type.TEXT)
        .add("col3", Type.FLOAT4)
        .build();
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|0.1",
        "2||",
        "3|filled|0.2"
    };

    try (ResultSet res = TajoTestingCluster
            .run(table, schemas, new String[][]{data},
                    "select * from nulltable1 where col3 is null", client)) {
      assertTrue(res.next());
      assertEquals(2, res.getInt(1));
      assertFalse(res.next());
    }
  }

  @Test
  public final void testIsNotNull() throws Exception {
    String [] table = new String[] {"nulltable2"};
    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.INT4)
        .add("col2", Type.TEXT)
        .build();
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|",
        "||",
        "3|filled|"
    };
    try (ResultSet res = TajoTestingCluster
            .run(table, schemas, new String[][]{data},
                    "select * from nulltable2 where col1 is not null", client)) {
      assertTrue(res.next());
      assertEquals(1, res.getInt(1));
      assertTrue(res.next());
      assertEquals(3, res.getInt(1));
      assertFalse(res.next());
    }
  }

  @Test
  public final void testIsNotNull2() throws Exception {
    String [] table = new String[] {"nulltable3"};
    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.INT8)
        .add("col2", Type.INT8)
        .add("col3", Type.INT8)
        .add("col4", Type.INT8)
        .add("col5", Type.INT8)
        .add("col6", Type.INT8)
        .add("col7", Type.INT8)
        .add("col8", Type.INT8)
        .add("col9", Type.INT8)
        .add("col10", Type.INT8)
        .build();
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "||||672287821|1301460|1|313895860387|126288907|1024",
        "|||43578|19|13|6|3581|2557|1024"
    };
    try (ResultSet res = TajoTestingCluster
            .run(table, schemas, new String[][]{data},
                    "select * from nulltable3 where col1 is null and col2 is null and col3 is null and col4 = 43578", client)) {
      assertTrue(res.next());
      assertEquals(43578, res.getLong(4));
      assertFalse(res.next());
    }
  }

  @Test
  public final void testIsNotNull3() throws Exception {
    String [] table = new String[] {"nulltable4"};
    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.INT8)
        .add("col2", Type.INT8)
        .add("col3", Type.INT8)
        .add("col4", Type.INT8)
        .add("col5", Type.INT8)
        .add("col6", Type.INT8)
        .add("col7", Type.INT8)
        .add("col8", Type.INT8)
        .add("col9", Type.INT8)
        .add("col10", Type.INT8)
        .build();
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "\\N||||672287821|",
        "|\\N||43578"
    };
    try (ResultSet res = TajoTestingCluster
            .run(table, schemas, new String[][]{data},
                    "select * from nulltable4 where col1 is null and col2 is null and col3 is null and col5 is null and col4 = 43578"
                    , client)) {
      assertTrue(res.next());
      assertEquals(43578, res.getLong(4));
      assertFalse(res.next());
    }
  }

  @Test
  public final void testResultSetNullSimpleQuery() throws Exception {
    String tableName = "nulltable5";
    ResultSet res = null;

    try {
      res = runNullTableQuery(tableName, "select col1, col2, col3, col4 from " + tableName, client);
      int numRows = 0;

      String expected =
              "null|a|1.0|true\n" +
              "2|null|2.0|false\n" +
              "3|c|null|true\n" +
              "4|d|4.0|null";

      String result = "";

      String prefix = "";
      while(res.next()) {
        for (int i = 0; i < 4; i++) {
          result += prefix + res.getObject(i + 1);
          prefix = "|";
        }
        prefix = "\n";

        assertResultSetNull(res, numRows, false, new int[]{1,2,3,4});
        assertResultSetNull(res, numRows, true, new int[]{1,2,3,4});
        numRows++;
      }
      assertEquals(4, numRows);
      assertEquals(expected, result);
    } finally {
      if (res != null) {
        res.close();
      }
    }
  }

  @Test
  public final void testResultSetNull() throws Exception {
    String tableName = "nulltable6";
    String query = "select " +
        "col1, coalesce(col1, 99999), " +
        "col2, coalesce(col2, 'null_value'), " +
        "col3, coalesce(col3, 99999.0)," +
        "col4 " +
        "from " + tableName;

    ResultSet res = null;

    try {
      res = runNullTableQuery(tableName, query, client);
      int numRows = 0;
      String expected =
          "null|99999|a|a|1.0|1.0|true\n" +
          "2|2|null|null_value|2.0|2.0|false\n" +
          "3|3|c|c|null|99999.0|true\n" +
          "4|4|d|d|4.0|4.0|null";

      String result = "";

      String prefix = "";
      while(res.next()) {
        for (int i = 0; i < 7; i++) {
          result += prefix + res.getObject(i + 1);
          prefix = "|";
        }
        prefix = "\n";

        assertResultSetNull(res, numRows, false, new int[]{1,3,5,7});
        assertResultSetNull(res, numRows, true, new int[]{1,3,5,7});
        numRows++;
      }
      assertEquals(4, numRows);
      assertEquals(expected, result);
    } finally {
      if (res != null) {
        res.close();
      }
    }
  }

  private ResultSet runNullTableQuery(String tableName, String query, TajoClient client) throws Exception {
    String [] table = new String[] {tableName};
    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.INT4)
        .add("col2", Type.TEXT)
        .add("col3", Type.FLOAT4)
        .add("col4", Type.BOOLEAN)
        .build();
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "\\N|a|1.0|t",
        "2|\\N|2.0|f",
        "3|c|\\N|t",
        "4|d|4.0|\\N"
    };

    if (client == null) {
      return TajoTestingCluster.run(table, schemas, new String[][]{data}, query);
    } else {
      return TajoTestingCluster.run(table, schemas, new String[][]{data}, query, client);
    }
  }

  private void assertResultSetNull(ResultSet res, int numRows, boolean useName, int[] nullIndex) throws SQLException {
    if (numRows == 0) {
      if (useName) {
        assertEquals(0, res.getInt(res.getMetaData().getColumnName(nullIndex[numRows])));
      } else {
        assertEquals(0, res.getInt(nullIndex[numRows]));
      }
    }

    if (numRows == 1) {
      if (useName) {
        assertNull(res.getString(res.getMetaData().getColumnName(nullIndex[numRows])));
      } else {
        assertNull(res.getString(nullIndex[numRows]));
      };
    }

    if (numRows == 2) {
      if (useName) {
        assertEquals(0.0, res.getDouble(res.getMetaData().getColumnName(nullIndex[numRows])), 10);
      } else {
        assertEquals(0.0, res.getDouble(nullIndex[numRows]), 10);
      }
    }

    if (numRows == 3) {
      if (useName) {
        assertEquals(false, res.getBoolean(res.getMetaData().getColumnName(nullIndex[numRows])));
      } else {
        assertEquals(false, res.getBoolean(nullIndex[numRows]));
      }
    }
  }
}
