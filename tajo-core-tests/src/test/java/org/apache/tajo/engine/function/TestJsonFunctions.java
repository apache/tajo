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


import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.apache.tajo.exception.TajoException;
import org.junit.Test;

public class TestJsonFunctions extends ExprTestBase {
  static final String JSON_DOCUMENT = "{\"map\" : {\"name\" : \"tajo\"}, \"array\" : [1,2,3]}";
  static final String JSON_ARRAY = "[100,200,300,400,500]";
  static final String JSON_COMPLEX_ARRAY = "[100, \"test\", \"2015-08-13 11:58:59\", 0.899999999999]";
  static final String JSON_EMPTY_ARRAY = "[]";

  @Test
  public void testJsonExtractPathText() throws TajoException {
    testSimpleEval("select json_extract_path_text('" + JSON_DOCUMENT + "', '$.map.name') ", new String[]{"tajo"});
    testSimpleEval("select json_extract_path_text('" + JSON_DOCUMENT + "', '$.array[1]') ", new String[]{"2"});
  }

  @Test
  public void testJsonArrayGet() throws TajoException {
    testSimpleEval("select json_array_get('" + JSON_ARRAY + "', 0)", new String[]{"100"});
    testSimpleEval("select json_array_get('" + JSON_ARRAY + "', 2)", new String[]{"300"});
    testSimpleEval("select json_array_get('" + JSON_ARRAY + "', -1)", new String[]{"500"});
    testSimpleEval("select json_array_get('" + JSON_ARRAY + "', -2)", new String[]{"400"});
    testSimpleEval("select json_array_get('" + JSON_ARRAY + "', 10)", new String[]{NullDatum.get().toString()});
    testSimpleEval("select json_array_get('" + JSON_ARRAY + "', -10)", new String[]{NullDatum.get().toString()});
    testSimpleEval("select json_array_get('" + JSON_EMPTY_ARRAY + "', 0)", new String[]{NullDatum.get().toString()});
  }

  @Test
  public void testJsonArrayContains() throws TajoException {
    testSimpleEval("select json_array_contains('" + JSON_COMPLEX_ARRAY + "', 100)", new String[]{"t"});
    testSimpleEval("select json_array_contains('" + JSON_COMPLEX_ARRAY + "', 'test')", new String[]{"t"});
    testSimpleEval("select json_array_contains('" + JSON_COMPLEX_ARRAY + "', '2015-08-13 11:58:59'::timestamp)",
        new String[]{"t"});
    testSimpleEval("select json_array_contains('" + JSON_COMPLEX_ARRAY + "', '2015-08-13 11:58:59'::date)",
        new String[]{"f"});
    testSimpleEval("select json_array_contains('" + JSON_COMPLEX_ARRAY + "', 1000)", new String[]{"f"});
    testSimpleEval("select json_array_contains('" + JSON_COMPLEX_ARRAY + "', 0.899999999999)", new String[]{"t"});
  }

  @Test
  public void testJsonArrayLength() throws TajoException {
    testSimpleEval("select json_array_length('" + JSON_ARRAY + "')", new String[]{"5"});
    testSimpleEval("select json_array_length('" + JSON_EMPTY_ARRAY + "')", new String[]{"0"});
  }
}
