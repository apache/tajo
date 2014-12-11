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

import org.junit.Test;

import java.io.IOException;

public class TestSQLDateTimeTypes extends ExprTestBase {

  @Test
  public void testTimestamp() throws IOException {
    testSimpleEval("select TIMESTAMP '1970-01-17 10:09:37';", new String[]{"1970-01-17 10:09:37"});
    testSimpleEval("select TIMESTAMP '1970-01-17 10:09:37.5';", new String[]{"1970-01-17 10:09:37.5"});
    testSimpleEval("select TIMESTAMP '1970-01-17 10:09:37.01';", new String[]{"1970-01-17 10:09:37.01"});
    testSimpleEval("select TIMESTAMP '1970-01-17 10:09:37.003';",new String[]{"1970-01-17 10:09:37.003"});
  }

  @Test
  public void testToTimestamp() throws IOException {
    testSimpleEval("select to_char(TIMESTAMP '1970-01-17 10:09:37', 'YYYY-MM-DD HH24:MI:SS');",
        new String[]{"1970-01-17 10:09:37"});
  }

  @Test
  public void testTimeLiteral() throws IOException {
    testSimpleEval("select TIME '10:09:37';", new String[]{"10:09:37"});
  }

  @Test
  public void testDateLiteral() throws IOException {
    testSimpleEval("select DATE '1970-01-17';", new String[]{"1970-01-17"});
  }

}
