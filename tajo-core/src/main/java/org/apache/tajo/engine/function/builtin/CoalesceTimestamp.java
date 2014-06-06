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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;

@Description(
    functionName = "coalesce",
    description = "Returns the first of its arguments that is not null.",
    detail = "Like a CASE expression, COALESCE only evaluates the arguments that are needed to determine the result; " +
        "that is, arguments to the right of the first non-null argument are not evaluated",
    example = "> SELECT coalesce(null, null, timestamp '2014-01-01');\n"
        + "2014-01-01 00:00:00",
    returnType = Type.TIMESTAMP,
    paramTypes = {@ParamTypes(paramTypes = {Type.TIMESTAMP, Type.TIMESTAMP_ARRAY})}
)
public class CoalesceTimestamp extends Coalesce {
  public CoalesceTimestamp() {
    super(new Column[] {
        new Column("column", TajoDataTypes.Type.TIMESTAMP),
        new Column("params", TajoDataTypes.Type.TIMESTAMP_ARRAY),
    });
  }
}
