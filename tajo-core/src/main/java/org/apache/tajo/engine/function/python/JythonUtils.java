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

package org.apache.tajo.engine.function.python;

import org.apache.tajo.catalog.Schema;

import java.util.HashMap;

public class JythonUtils {

//  /**
//   * @param schemaString a String representation of the Schema <b>without</b>
//   *                     any enclosing curly-braces.<b>Not</b> for use with
//   *                     <code>Schema#toString</code>
//   * @return Schema instance
//   * @throws ParserException
//   */
//  public static Schema getSchemaFromString(String schemaString) {
//    LogicalSchema schema = parseSchema(schemaString);
//    Schema result = org.apache.pig.newplan.logical.Util.translateSchema(schema);
//    Schema.setSchemaDefaultType(result, DataType.BYTEARRAY);
//    return result;
//  }
//
//  public static LogicalSchema parseSchema(String schemaString) {
//    QueryParserDriver queryParser = new QueryParserDriver( new PigContext(),
//        "util", new HashMap<String, String>() ) ;
//    LogicalSchema schema = queryParser.parseSchema(schemaString);
//    return schema;
//  }
}
