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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.TajoException;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

public class TestPredicates extends ExprTestBase {

  public static final Schema FourIntSchema;

  static {
    FourIntSchema = SchemaBuilder.builder()
        .add("col1", INT4)
        .add("col2", INT4)
        .add("col3", INT4)
        .add("col4", INT4)
        .build();
  }

  //////////////////////////////////////////////////////////////////
  // Logical Operator
  //////////////////////////////////////////////////////////////////

  @Test
  public void testAnd() throws TajoException {
    testSimpleEval("select true;", new String[] {"t"});

    testSimpleEval("select true and true;", new String[] {"t"});
    testSimpleEval("select true and false;", new String[] {"f"});
    testSimpleEval("select false and true;", new String[] {"f"});
    testSimpleEval("select false and false;", new String[] {"f"});
  }

  @Test
  public void testOr() throws TajoException {
    testSimpleEval("select true or true;", new String[] {"t"});
    testSimpleEval("select true or false;", new String[] {"t"});
    testSimpleEval("select false or true;", new String[] {"t"});
    testSimpleEval("select false or false;", new String[] {"f"});
  }

  @Test
  public void testLogicalOperatorPrecedence() throws TajoException {
    testSimpleEval("select true or (false or false) or false;", new String[] {"t"});
    testSimpleEval("select false or (true or false) or false;", new String[] {"t"});
    testSimpleEval("select false or (false or true) or false;", new String[] {"t"});
    testSimpleEval("select false or (false or false) or true;", new String[] {"t"});

    testSimpleEval("select true and (false or false) or false;", new String[] {"f"});
    testSimpleEval("select false and (true or false) or false;", new String[] {"f"});
    testSimpleEval("select false and (false or true) or false;", new String[] {"f"});
    testSimpleEval("select false and (false or false) or true;", new String[] {"t"});

    testSimpleEval("select true or (false and false) or false;", new String[] {"t"});
    testSimpleEval("select false or (true and false) or false;", new String[] {"f"});
    testSimpleEval("select false or (false and true) or false;", new String[] {"f"});
    testSimpleEval("select false or (false and true) or true;", new String[] {"t"});

    testSimpleEval("select true or (false or false) and false;", new String[] {"t"});
    testSimpleEval("select false or (true or false) and false;", new String[] {"f"});
    testSimpleEval("select false or (false or true) and false;", new String[] {"f"});
    testSimpleEval("select false or (false or false) and true;", new String[] {"f"});
  }

  @Test
  public void testNot() throws TajoException {

    testSimpleEval("select true;", new String[] {"t"});
    testSimpleEval("select not true;", new String[] {"f"});
    testSimpleEval("select (true);", new String[] {"t"});
    testSimpleEval("select not (true);", new String[] {"f"});
    testSimpleEval("select not (not (true));", new String[] {"t"});

    testSimpleEval("select (not (1 > null)) is null;", new String[] {"t"});

    Schema schema1 = SchemaBuilder.builder()
        .add("col1", INT4)
        .add("col2", INT4)
        .add("col3", INT4)
        .build();

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 = col2, col1 = col3 from table1",
        new String[]{"t", "f"});
  }

  @Test
  public void testParenthesizedValues() throws TajoException {
    testSimpleEval("select ((true));", new String[] {"t"});
    testSimpleEval("select ((((true))));", new String[] {"t"});
    testSimpleEval("select not(not(not(false)));", new String[] {"t"});
  }

  //////////////////////////////////////////////////////////////////
  // Comparison Predicate
  //////////////////////////////////////////////////////////////////

  @Test
  public void testComparisonEqual() throws TajoException {
    Schema schema = SchemaBuilder.builder()
        .add("col0", TajoDataTypes.Type.INT1)
        .add("col1", TajoDataTypes.Type.INT2)
        .add("col2", TajoDataTypes.Type.INT4)
        .add("col3", TajoDataTypes.Type.INT8)
        .add("col4", TajoDataTypes.Type.FLOAT4)
        .add("col5", TajoDataTypes.Type.FLOAT8)
        .add("col6", TajoDataTypes.Type.TEXT)
        .add("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3))
        .add("nullable", TajoDataTypes.Type.INT4)
        .build();

    testEval(schema, "t1", "0,1,2,3,4.1,5.1,cmp,asm,", "SELECT col6 = 'cmp' from t1", new String [] {"t"});

    Schema schema1 = FourIntSchema;

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 = col2, col1 = col3, col1 = col4 from table1",
        new String[]{"t", "f", "f"});
    testEval(schema1,
        "table1", "123,123,,",
        "select col1 = col2, (col1 = col3) is null, (col3 = col2) is null from table1",
        new String[]{"t", "t", "t"});
  }

  @Test
  public void testComparisonNotEqual() throws TajoException {
    Schema schema1 = FourIntSchema;

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 <> col2, col1 <> col3, col1 <> col4 from table1",
        new String[]{"f", "t", "t"});
    testEval(schema1,
        "table1", "123,123,,",
        "select col1 <> col2, (col1 <> col3) is null, (col3 <> col2) is null from table1",
        new String[]{"f", "t", "t"});
  }

  @Test
  public void testComparisonLessThan() throws TajoException {
    Schema schema1 = FourIntSchema;

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 < col2, col1 < col3, col1 < col4 from table1",
        new String[]{"f", "t", "f"});
    testEval(schema1,
        "table1", "123,456,,",
        "select col1 < col2, (col1 = col3) is null, (col4 = col1) is null from table1",
        new String[]{"t", "t", "t"});
  }

  @Test
  public void testComparisonLessThanEqual() throws TajoException {
    Schema schema1 = FourIntSchema;

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 <= col2, col1 <= col3, col1 <= col4 from table1",
        new String[]{"t", "t", "f"});
    testEval(schema1,
        "table1", "123,456,,",
        "select col1 <= col2, (col1 <= col3) is null, (col4 <= col1) is null from table1",
        new String[]{"t", "t", "t"});
  }

  @Test
  public void testComparisonGreaterThan() throws TajoException {
    Schema schema1 = FourIntSchema;

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 > col2, col3 > col2, col1 > col4 from table1",
        new String[]{"f", "t", "t"});
    testEval(schema1,
        "table1", "123,456,,",
        "select col2 > col1, col1 > col2, (col1 > col3) is null, (col4 > col1) is null from table1",
        new String[]{"t", "f", "t", "t"});
  }

  @Test
  public void testComparisonGreaterThanEqual() throws TajoException {
    Schema schema1 = FourIntSchema;

    testEval(schema1,
        "table1", "123,123,456,-123",
        "select col1 >= col2, col3 >= col2, col1 >= col4 from table1",
        new String[]{"t", "t", "t"});
    testEval(schema1,
        "table1", "123,456,,",
        "select col2 >= col1, col1 >= col2, (col1 >= col3) is null, (col4 >= col1) is null from table1",
        new String[]{"t", "f", "t", "t"});
  }

  //////////////////////////////////////////////////////////////////
  // Between Predicate
  //////////////////////////////////////////////////////////////////

  @Test
  public void testBetween() throws TajoException {
    Schema schema2 = SchemaBuilder.builder()
        .add("col1", TEXT)
        .add("col2", TEXT)
        .add("col3", TEXT)
        .build();

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
  public void testBetween2() throws TajoException { // for TAJO-249
    Schema schema3 = SchemaBuilder.builder()
        .add("date_a", INT4)
        .add("date_b", INT4)
        .add("date_c", INT4)
        .add("date_d", INT4)
        .build();

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

  //////////////////////////////////////////////////////////////////
  // In Predicate
  //////////////////////////////////////////////////////////////////

  @Test
  public void testInPredicateWithConstant() throws TajoException {
    Schema schema2 = SchemaBuilder.builder()
        .add("col1", TEXT)
        .add("col2", TEXT)
        .add("col3", TEXT)
        .build();

    testEval(schema2, "table1", "a,b,c", "select col1 in ('a'), col2 in ('a', 'c') from table1", new String[]{"t","f"});
    testEval(schema2, "table1", "a,\\NULL,c", "select col1 in ('a','b','c'), (col2 in ('a', 'c')) is null from table1",
        new String[]{"t","t"});

    testEval(schema2,
        "table1",
        "2014-03-21,2015-04-01,2016-04-01",
        "select substr(col1,1,4) in ('2014','2015','2016'), substr(col1,6,2)::int4 in (1,2,3) from table1",
        new String[]{"t", "t"});

    // null handling test
    testEval(schema2,
        "table1",
        "2014-03-21,\\NULL,2015-04-01",
        "select (substr(col2,1,4)::int4 in (2014,2015,2016)) is null from table1",
        new String[]{"t"});
  }

  @Test
  public void testInPredicateWithSimpleExprs() throws TajoException {
    Schema schema2 = SchemaBuilder.builder()
        .add("col1", TEXT)
        .add("col2", INT4)
        .add("col3", TEXT)
        .build();

    testEval(schema2, "table1", "abc,2,3", "select col1 in ('a'||'b'||'c'), col2 in (1 + 1, 2 * 10, 2003) from table1",
        new String[]{"t","t"});

    testEval(schema2, "table1", "abc,2,3", "select col1 in ('a'||'b'), col2 in ('1'::int, '2'::int, 3) from table1",
        new String[]{"f","t"});

    testEval(schema2,
        "table1",
        "abc,,3",
        "select col1 in (reverse('cba')), (col2 in ('1'::int, '2'::int, 3)) is null from table1",
        new String[]{"t","t"});
  }

  //////////////////////////////////////////////////////////////////
  // Null Predicate
  //////////////////////////////////////////////////////////////////

  @Test
  public void testIsNullPredicate() throws TajoException {
    Schema schema1 = SchemaBuilder.builder()
        .add("col1", INT4)
        .add("col2", INT4)
        .build();
    testEval(schema1, "table1", "123,", "select col1 is null, col2 is null as a from table1",
        new String[]{"f", "t"});
    testEval(schema1, "table1", "123,", "select col1 is not null, col2 is not null as a from table1",
        new String[]{"t", "f"});
  }

  @Test
  public void testIsNullPredicateWithFunction() throws TajoException {
    Schema schema2 = SchemaBuilder.builder()
        .add("col1", TEXT)
        .add("col2", TEXT)
        .build();
    testEval(schema2, "table1", "_123,\\NULL", "select ltrim(col1, '_') is null, upper(col2) is null as a from table1",
        new String[]{"f", "t"});

    testEval(schema2, "table1", "_123,\\NULL",
        "select ltrim(col1, '_') is not null, upper(col2) is not null as a from table1", new String[]{"t", "f"});
  }

  //////////////////////////////////////////////////////////////////
  // Boolean Test
  //////////////////////////////////////////////////////////////////

  @Test
  public void testBooleanTest() throws TajoException {
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

  @Test
  public void testBooleanTestOnTable() throws TajoException {
    Schema schema = SchemaBuilder.builder()
        .add("col1", BOOLEAN)
        .add("col2", BOOLEAN)
        .build();
    testEval(schema, "table1", "t,f", "select col1 is true, col2 is false from table1", new String [] {"t", "t"});
    testEval(schema, "table1", "t,f", "select col1 is not true, col2 is not false from table1",
        new String [] {"f", "f"});
    testEval(schema, "table1", "t,f", "select not col1 is not true, not col2 is not false from table1",
        new String [] {"t", "t"});
  }
}
