/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.function.json;

import net.minidev.json.parser.JSONParser;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;

public abstract class ScalarJsonFunction extends GeneralFunction {
  protected JSONParser parser;

  public ScalarJsonFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public void init(OverridableConf queryContext, FunctionEval.ParamType [] paramTypes) {
    parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);
  }
}
