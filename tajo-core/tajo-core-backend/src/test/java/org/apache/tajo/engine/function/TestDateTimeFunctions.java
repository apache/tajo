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


import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import java.io.IOException;

public class TestDateTimeFunctions extends ExprTestBase {

  @Test
  public void testToTimestamp() throws IOException {
    testSimpleEval("select to_timestamp(cast (1386577582 as int8));", new String[]{"1970-01-17 10:09:37"});
    testSimpleEval("select to_timestamp(cast ('1386577582' as int8));", new String[]{"1970-01-17 10:09:37"});
    testSimpleEval("select to_timestamp(cast (1386577582 as int8)) < to_timestamp(cast (1386577583 as int8));",
        new String[]{"t"});
  }

  @Test
  public void testToChar() throws IOException {
    testSimpleEval("select to_char(to_timestamp(1386577582), 'yyyy-MM');", new String[]{"1970-01"});
  }
}
