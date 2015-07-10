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

package org.apache.tajo.engine.function.builtin;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Count(column) function
 */
@Description(
  functionName = "count",
  description = "The number of retrieved rows for "
          + "which the supplied expressions are non-NULL",
  example = "> SELECT count(expr);",
  returnType = Type.INT8,
  paramTypes = {@ParamTypes(paramTypes = {Type.ANY})}
)
public final class CountValue extends CountRows {

  public CountValue() {
    super(new Column[] {
        new Column("expr", Type.ANY)
    });
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    if (!params.isBlankOrNull(0)) {
      ((CountRowContext) ctx).count++;
    }
  }

}
