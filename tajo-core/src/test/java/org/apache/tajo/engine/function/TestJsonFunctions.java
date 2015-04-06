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

public class TestJsonFunctions extends ExprTestBase {
  static final String JSON_DOCUMENT = "{\"map\" : {\"name\" : \"tajo\"}, \"array\" : [1,2,3]}";

  @Test
  public void testJsonExtractPathText() throws IOException {
    testSimpleEval("select json_extract_path_text('" + JSON_DOCUMENT + "', '$.map.name') ", new String[]{"tajo"});
    testSimpleEval("select json_extract_path_text('" + JSON_DOCUMENT + "', '$.array[1]') ", new String[]{"2"});

  }
}
