/*
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

import static org.apache.tajo.common.TajoDataTypes.Type.BOOLEAN;

public class TestUserDefinedFunctions extends ExprTestBase {

  @Test
  public void testNullHandling() throws TajoException {
    testSimpleEval("select null_test()", new String[]{NullDatum.get().toString()});
  }

  @Test
  public void testNullHandling2() throws TajoException {
    Schema schema = new Schema();
    schema.addColumn("col1", BOOLEAN);

    testEval(schema, "table1", "", "select null_test() from table1", new String[]{NullDatum.get().toString()});
  }
}
