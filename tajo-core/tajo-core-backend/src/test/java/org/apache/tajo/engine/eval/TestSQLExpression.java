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

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestSQLExpression extends ExprTestBase {

  @Test
  public void testCast() {
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
  public void testCastFromTable() {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    testEval(schema, "table1", "123,234", "select cast(col1 as float) as b, cast(col2 as float) as a from table1",
        new String[]{"123.0", "234.0"});
    testEval(schema, "table1", "123,234", "select col1::float, col2::float from table1",
        new String[]{"123.0", "234.0"});
  }
}
