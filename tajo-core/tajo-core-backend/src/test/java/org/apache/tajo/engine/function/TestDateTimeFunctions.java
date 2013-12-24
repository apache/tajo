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


import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;

public class TestDateTimeFunctions extends ExprTestBase {

  @Test
  public void testToTimestamp() throws IOException {
    long expectedTimestamp = System.currentTimeMillis();
    DateTime expectedDateTime = new DateTime(expectedTimestamp);

    // (expectedTimestamp / 1000) means the translation from millis seconds to unix timestamp
    String q1 = String.format("select to_timestamp(%d);", (expectedTimestamp / 1000));
    testSimpleEval(q1, new String[]{expectedDateTime.toString(TimestampDatum.DEFAULT_FORMAT_STRING)});
  }

  @Test
  public void testToChar() throws IOException {
    long expectedTimestamp = System.currentTimeMillis();
    DateTime expectedDateTime = new DateTime(expectedTimestamp);
    String dateFormatStr = "yyyy-MM";
    // (expectedTimestamp / 1000) means the translation from millis seconds to unix timestamp
    String q = String.format("select to_char(to_timestamp(%d), 'yyyy-MM');", (expectedTimestamp / 1000));
    testSimpleEval(q, new String[]{expectedDateTime.toString(dateFormatStr)});
  }
}
