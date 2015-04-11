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

import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import java.io.IOException;

public class TestPythonFunctions extends ExprTestBase {

  @Test
  public void testFunctions() throws IOException {
    testSimpleEval("select return_one()", new String[]{"1"});
    testSimpleEval("select helloworld()", new String[]{"Hello, World"});
    testSimpleEval("select concat_py('1')", new String[]{"11"});
    testSimpleEval("select comma_format(12345)", new String[]{"12,345"});
    testSimpleEval("select sum_py(1,2)", new String[]{"3"});
    testSimpleEval("select percent(386, 1000)", new String[]{"38.6"});
    testSimpleEval("select concat4('Tajo', 'is', 'awesome', '!')", new String[]{"Tajo is awesome !"});
  }

  @Test
  public void testNestedFunctions() throws IOException {
    testSimpleEval("select sum_py(3, return_one())", new String[]{"4"});
    testSimpleEval("select concat_py(helloworld())", new String[]{"Hello, WorldHello, World"});
  }
}
