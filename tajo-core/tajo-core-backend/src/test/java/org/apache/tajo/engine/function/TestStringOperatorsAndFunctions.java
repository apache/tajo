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
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.FLOAT8;
import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestStringOperatorsAndFunctions extends ExprTestBase {

  @Test
  public void testConcatenateOnLiteral() {
    testSimpleEval("select ('abc' || 'def') col1 ", new String[]{"abcdef"});
    testSimpleEval("select 'abc' || 'def' as col1 ", new String[]{"abcdef"});
    testSimpleEval("select 1 || 'def' as col1 ", new String[]{"1def"});
    testSimpleEval("select 'abc' || 2 as col1 ", new String[]{"abc2"});
  }

  @Test
  public void testConcatenateOnExpressions() {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", INT4);
    schema.addColumn("col3", FLOAT8);

    testSimpleEval("select (1+3) || 2 as col1 ", new String[]{"42"});

    testEval(schema, "table1", "abc,2,3.14", "select col1 || col2 || col3 from table1", new String[]{"abc23.14"});
    testEval(schema, "table1", "abc,2,3.14", "select col1 || '---' || col3 from table1", new String[]{"abc---3.14"});
  }
}
