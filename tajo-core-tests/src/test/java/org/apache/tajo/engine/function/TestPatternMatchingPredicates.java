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

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestPatternMatchingPredicates extends ExprTestBase {

  @Test
  public void testLike() throws TajoException {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);

    // test for null values
    testEval(schema, "table1", ",", "select col1 like 'a%' from table1", new String[]{NullDatum.get().toString()});
    testSimpleEval("select null like 'a%'", new String[]{NullDatum.get().toString()});

    testEval(schema, "table1", "abc", "select col1 like '%c' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 like 'a%' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 like '_bc' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 like 'ab_' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 like '_b_' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 like '%b%' from table1", new String[]{"t"});

    // test for escaping regular expressions
    testEval(schema, "table1", "abc", "select col1 not like '.bc' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 not like '.*bc' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 not like '.bc' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 not like '*bc' from table1", new String[]{"t"});

    // test for case sensitive
    testEval(schema, "table1", "abc", "select col1 not like '%C' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 not like 'A%' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 not like '_BC' from table1", new String[]{"t"});
    testEval(schema, "table1", "abc", "select col1 not like '_C_' from table1", new String[]{"t"});
  }

  @Test
  public void testILike() throws TajoException {
    testSimpleEval("select 'abc' ilike '%c'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike 'a%'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike '_bc'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike 'ab_'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike '_b_'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike '%b%'", new String[]{"t"});

    // test for escaping regular expressions
    testSimpleEval("select 'abc' not like '.bc'", new String[]{"t"});
    testSimpleEval("select 'abc' not like '.*bc'", new String[]{"t"});
    testSimpleEval("select 'abc' not like '.bc'", new String[]{"t"});
    testSimpleEval("select 'abc' not like '*bc'", new String[]{"t"});

    // test for case insensitive
    testSimpleEval("select 'abc' ilike '%C'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike 'A%'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike '_BC'", new String[]{"t"});
    testSimpleEval("select 'abc' ilike '_B_'", new String[]{"t"});
  }

  @Test
  public void testSimilarToLike() throws TajoException {
    testSimpleEval("select 'abc' similar to '%c'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to 'a%'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to '_bc'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to 'ab_'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to '_b_'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to '%b%'", new String[]{"t"});

    // test for now allowed
    testSimpleEval("select 'abc' not similar to '.bc'", new String[]{"t"});
    testSimpleEval("select 'abc' not similar to 'ab.'", new String[]{"t"});

    // test for escaping regular expressions
    testSimpleEval("select 'abc' similar to '(a|f)b%'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to '[a-z]b%'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to '_+bc'", new String[]{"t"});
    testSimpleEval("select 'abc' similar to 'abc'", new String[]{"t"});

    // test for case sensitive
    testSimpleEval("select 'abc' not similar to '%C'", new String[]{"t"});
    testSimpleEval("select 'abc' not similar to '_Bc'", new String[]{"t"});
  }

  @Test
  public void testRegexWithSimilarOperator() throws TajoException {
    testSimpleEval("select 'abc' ~ '.*c'", new String[]{"t"});
    testSimpleEval("select 'abc' ~ '.*c$'", new String[]{"t"});
    testSimpleEval("select 'aaabc' ~ '([a-z]){3}bc'", new String[]{"t"});

    // for negative condition
    testSimpleEval("select 'abc' !~ '.*c$'", new String[]{"f"});

    // for case sensitivity
    testSimpleEval("select 'abc' ~ '.*C'", new String[]{"f"});

    // for case insensitivity
    testSimpleEval("select 'abc' ~* '.*C'", new String[]{"t"});
    testSimpleEval("select 'abc' !~* '.*C'", new String[]{"f"});
  }

  @Test
  public void testRegexp() throws TajoException {
    testSimpleEval("select 'abc' regexp '.*c'", new String[]{"t"});
    testSimpleEval("select 'abc' regexp '.*c$'", new String[]{"t"});

    // for negative condition
    testSimpleEval("select 'abc' not regexp '.*c$'", new String[]{"f"});
  }

  @Test
  public void testRLike() throws TajoException {
    testSimpleEval("select 'abc' rlike '.*c'", new String[]{"t"});
    testSimpleEval("select 'abc' rlike '.*c$'", new String[]{"t"});

    // for negative condition
    testSimpleEval("select 'abc' not rlike '.*c$'", new String[]{"f"});
  }
}
