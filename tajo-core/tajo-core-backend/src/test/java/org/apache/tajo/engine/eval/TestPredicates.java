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

package org.apache.tajo.engine.eval;

import org.apache.tajo.catalog.Schema;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestPredicates extends ExprTestBase {
  @Test
  public void testIsNullPredicate() throws IOException {
    Schema schema1 = new Schema();
    schema1.addColumn("col1", INT4);
    schema1.addColumn("col2", INT4);
    testEval(schema1, "table1", "123,", "select col1 is null, col2 is null as a from table1",
        new String[]{"f", "t"});
    testEval(schema1, "table1", "123,", "select col1 is not null, col2 is not null as a from table1",
        new String[]{"t", "f"});
  }

  @Test
  public void testIsNullPredicateWithFunction() throws IOException {
    Schema schema2 = new Schema();
    schema2.addColumn("col1", TEXT);
    schema2.addColumn("col2", TEXT);
    testEval(schema2, "table1", "_123,", "select ltrim(col1, '_') is null, upper(col2) is null as a from table1",
        new String[]{"f", "t"});

    testEval(schema2, "table1", "_123,",
        "select ltrim(col1, '_') is not null, upper(col2) is not null as a from table1", new String[]{"t", "f"});
  }

  @Test
  public void testBetween() throws IOException {
    Schema schema2 = new Schema();
    schema2.addColumn("col1", TEXT);
    schema2.addColumn("col2", TEXT);
    schema2.addColumn("col3", TEXT);

    // constant checker
    testEval(schema2, "table1", "b,a,c", "select col1 between 'a' and 'c' from table1", new String[]{"t"});
    testEval(schema2, "table1", "b,a,c", "select col1 between 'c' and 'a' from table1", new String[]{"f"});
    testEval(schema2, "table1", "b,a,c", "select col1 between symmetric 'c' and 'a' from table1", new String[]{"t"});
    testEval(schema2, "table1", "d,a,c", "select col1 between 'a' and 'c' from table1", new String[]{"f"});

    // tests for inclusive
    testEval(schema2, "table1", "a,a,c", "select col1 between col2 and col3 from table1", new String[]{"t"});
    testEval(schema2, "table1", "b,a,c", "select col1 between col2 and col3 from table1", new String[]{"t"});
    testEval(schema2, "table1", "c,a,c", "select col1 between col2 and col3 from table1", new String[]{"t"});
    testEval(schema2, "table1", "d,a,c", "select col1 between col2 and col3 from table1", new String[]{"f"});

    // tests for asymmetric and symmetric
    testEval(schema2, "table1", "b,a,c", "select col1 between col3 and col2 from table1", new String[]{"f"});
    testEval(schema2, "table1", "b,a,c", "select col1 between symmetric col3 and col2 from table1", new String[]{"t"});
  }

  @Test
  public void testBetween2() throws IOException { // for TAJO-249
    Schema schema3 = new Schema();
    schema3.addColumn("date_a", INT4);
    schema3.addColumn("date_b", INT4);
    schema3.addColumn("date_c", INT4);
    schema3.addColumn("date_d", INT4);

    String query = "select " +
        "case " +
        "when date_a BETWEEN 20130705 AND 20130715 AND ((date_b BETWEEN 20100101 AND 20120601) OR date_b > 20130715) " +
        "AND (date_c < 20120601 OR date_c > 20130715) AND date_d > 20130715" +
        "then 1 else 0 end from table1";

    testEval(schema3, "table1", "20130715,20100102,20120525,20130716", query, new String [] {"1"});
    testEval(schema3, "table1", "20130716,20100102,20120525,20130716", query, new String [] {"0"});

    // date_b
    testEval(schema3, "table1", "20130715,20100102,20120525,20130716", query, new String [] {"1"});
    testEval(schema3, "table1", "20130715,20120602,20120525,20130716", query, new String [] {"0"});
    testEval(schema3, "table1", "20130715,20091201,20120525,20130716", query, new String [] {"0"});
    testEval(schema3, "table1", "20130715,20130716,20120525,20130716", query, new String [] {"1"});

    // date_c
    testEval(schema3, "table1", "20130715,20100102,20120525,20130716", query, new String [] {"1"});
    testEval(schema3, "table1", "20130715,20100102,20120602,20130716", query, new String [] {"0"});

    testEval(schema3, "table1", "20130715,20100102,20130716,20130716", query, new String [] {"1"});
    testEval(schema3, "table1", "20130715,20100102,20130714,20130716", query, new String [] {"0"});

    // date_d
    testEval(schema3, "table1", "20130715,20100102,20120525,20130716", query, new String [] {"1"});
    testEval(schema3, "table1", "20130715,20100102,20120525,20130705", query, new String [] {"0"});
  }

  @Test
  public void testBooleanTest() throws IOException {
    testSimpleEval("select 1 < 3 is true", new String [] {"t"});
    testSimpleEval("select 1 < 3 is not true", new String [] {"f"});
    testSimpleEval("select 1 < 3 is false", new String [] {"f"});
    testSimpleEval("select 1 < 3 is not false", new String [] {"t"});

    testSimpleEval("select not (1 < 3 is true)", new String [] {"f"});
    testSimpleEval("select not (1 < 3 is not true)", new String [] {"t"});
    testSimpleEval("select not (1 < 3 is false)", new String [] {"t"});
    testSimpleEval("select not (1 < 3 is not false)", new String [] {"f"});

    testSimpleEval("select 1 > 3 is true", new String [] {"f"});
    testSimpleEval("select 1 > 3 is not true", new String [] {"t"});
    testSimpleEval("select 1 > 3 is false", new String [] {"t"});
    testSimpleEval("select 1 > 3 is not false", new String [] {"f"});

    testSimpleEval("select not (1 > 3 is true)", new String [] {"t"});
    testSimpleEval("select not (1 > 3 is not true)", new String [] {"f"});
    testSimpleEval("select not (1 > 3 is false)", new String [] {"f"});
    testSimpleEval("select not (1 > 3 is not false)", new String [] {"t"});

  }
}
