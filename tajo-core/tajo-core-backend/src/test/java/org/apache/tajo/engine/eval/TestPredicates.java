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

import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestPredicates extends ExprTestBase {
  @Test
  public void testIsNullPredicate() {
    Schema schema1 = new Schema();
    schema1.addColumn("col1", INT4);
    schema1.addColumn("col2", INT4);
    testEval(schema1, "table1", "123,", "select col1 is null, col2 is null as a from table1",
        new String[]{"f", "t"});
    testEval(schema1, "table1", "123,", "select col1 is not null, col2 is not null as a from table1",
        new String[]{"t", "f"});
  }

  @Test
  public void testIsNullPredicateWithFunction() {
    Schema schema2 = new Schema();
    schema2.addColumn("col1", TEXT);
    schema2.addColumn("col2", TEXT);
    testEval(schema2, "table1", "_123,", "select ltrim(col1, '_') is null, upper(col2) is null as a from table1",
        new String[]{"f", "t"});

    testEval(schema2, "table1", "_123,",
        "select ltrim(col1, '_') is not null, upper(col2) is not null as a from table1", new String[]{"t", "f"});
  }
}
