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
import org.apache.tajo.datum.TimestampDatum;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestSQLExpression extends ExprTestBase {

  @Test
  public void testCast() throws IOException {
    testSimpleEval("select cast (1 as char)", new String[] {"1"});
    testSimpleEval("select cast (119 as char)", new String[] {"1"});

    testSimpleEval("select cast (1 as int2)", new String[ ]{"1"});
    testSimpleEval("select cast (1 as int4)", new String[] {"1"});
    testSimpleEval("select cast (1 as int8)", new String[] {"1"});
    testSimpleEval("select cast (1 as float)", new String[] {"1.0"});
    testSimpleEval("select cast (1 as double)", new String[] {"1.0"});
    testSimpleEval("select cast (1 as text)", new String[] {"1"});

    testSimpleEval("select cast ('123' as int2)", new String[] {"123"});
    testSimpleEval("select cast ('123' as int4)", new String[] {"123"});
    testSimpleEval("select cast ('123' as int8)", new String[] {"123"});
    testSimpleEval("select cast ('123' as float)", new String[] {"123.0"});
    testSimpleEval("select cast ('123' as double)", new String[] {"123.0"});
    testSimpleEval("select cast ('123' as text)", new String[] {"123"});

    testSimpleEval("select 123::int2", new String[] {"123"});
    testSimpleEval("select 123::int4", new String[] {"123"});
    testSimpleEval("select 123::int8", new String[] {"123"});
    testSimpleEval("select 123::float", new String[] {"123.0"});
    testSimpleEval("select 123::double", new String[] {"123.0"});
    testSimpleEval("select 123::text", new String[] {"123"});

    testSimpleEval("select 123.0::float", new String[] {"123.0"});
    testSimpleEval("select 123.0::double", new String[] {"123.0"});

    testSimpleEval("select '123'::int", new String[] {"123"});
    testSimpleEval("select '123'::double", new String[] {"123.0"});
  }

  @Test
  public void testCastWithNestedFunction() throws IOException {
    int timestamp = (int) (System.currentTimeMillis() / 1000);
    TimestampDatum expected = new TimestampDatum(timestamp);
    testSimpleEval(String.format("select to_timestamp(CAST(split_part('%d.999', '.', 1) as INT8));", timestamp),
        new String[] {expected.asChars()});
  }

  @Test
  public void testCastFromTable() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    testEval(schema, "table1", "123,234", "select cast(col1 as float) as b, cast(col2 as float) as a from table1",
        new String[]{"123.0", "234.0"});
    testEval(schema, "table1", "123,234", "select col1::float, col2::float from table1",
        new String[]{"123.0", "234.0"});
    testEval(schema, "table1", "1980-04-01 01:50:01,234", "select col1::timestamp, col2::float from table1 " +
        "where col1 = '1980-04-01 01:50:01'::timestamp",
        new String[]{"1980-04-01 01:50:01", "234.0"});

    testSimpleEval("select '1980-04-01 01:50:01'::timestamp;", new String [] {"1980-04-01 01:50:01"});
    testSimpleEval("select '1980-04-01 01:50:01'::timestamp::text", new String [] {"1980-04-01 01:50:01"});

    testSimpleEval("select (cast ('99999'::int8 as text))::int4 + 1", new String [] {"100000"});
  }

  @Test
  public void testBooleanLiteral() throws IOException {
    testSimpleEval("select true", new String[] {"t"});
    testSimpleEval("select false", new String[]{"f"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    testEval(schema, "table1", "123,234", "select col1, col2 from table1 where true", new String[]{"123", "234"});
  }

  @Test
  public void testNullComparisons() throws IOException {
    testSimpleEval("select null is null", new String[] {"t"});
    testSimpleEval("select null is not null", new String[] {"f"});

    testSimpleEval("select (1::int2 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::int4 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::int8 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::float > null) is null", new String[] {"t"});
    testSimpleEval("select (1::float < null) is null", new String[] {"t"});
    testSimpleEval("select (1::float >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::float8 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 <> null) is null", new String[] {"t"});

    testSimpleEval("select ('abc' > null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' < null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' >= null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' <= null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' <> null) is null", new String[] {"t"});

    testSimpleEval("select ('1980-04-01'::date > null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date < null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date >= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date <= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date <> null) is null", new String[] {"t"});

    testSimpleEval("select ('09:08:50'::time > null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time < null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time >= null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time <= null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time <> null) is null", new String[] {"t"});

    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp > null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp < null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp >= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp <= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp <> null) is null", new String[] {"t"});


    // Three Valued Logic - AND
    testSimpleEval("select (true AND true)", new String[] {"t"}); // true - true -> true
    testSimpleEval("select (true AND 1 > null) is null", new String[] {"t"}); // true - unknown -> unknown
    testSimpleEval("select (true AND false)", new String[] {"f"}); // true - false -> true

    testSimpleEval("select (1 > null AND true) is null", new String[] {"t"}); // unknown - true -> true
    testSimpleEval("select (1 > null AND 1 > null) is null", new String[] {"t"}); // unknown - unknown -> unknown
    testSimpleEval("select (1 > null AND false)", new String[] {"f"}); // unknown - false -> false

    testSimpleEval("select (false AND true)", new String[] {"f"}); // false - true -> true
    testSimpleEval("select (false AND 1 > null) is null", new String[] {"f"}); // false - unknown -> unknown
    testSimpleEval("select (false AND false)", new String[] {"f"}); // false - false -> false

    // Three Valued Logic - OR
    testSimpleEval("select (true OR true)", new String[] {"t"}); // true - true -> true
    testSimpleEval("select (true OR 1 > null)", new String[] {"t"}); // true - unknown -> true
    testSimpleEval("select (true OR false)", new String[] {"t"}); // true - false -> true

    testSimpleEval("select (1 > null OR true)", new String[] {"t"}); // unknown - true -> true
    testSimpleEval("select (1 > null OR 1 > null) is null", new String[] {"t"}); // unknown - unknown -> unknown
    testSimpleEval("select (1 > null OR false) is null", new String[] {"t"}); // unknown - false -> false

    testSimpleEval("select (false OR true)", new String[] {"t"}); // false - true -> true
    testSimpleEval("select (false OR 1 > null) is null", new String[] {"t"}); // false - unknown -> unknown
    testSimpleEval("select (false OR false)", new String[] {"f"}); // false - false -> false
  }
}
