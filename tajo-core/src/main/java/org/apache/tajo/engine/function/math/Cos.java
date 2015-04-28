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

package org.apache.tajo.engine.function.math;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * Float8 cos(value FLOAT8)
 */
@Description(
  functionName = "cos",
  description = "Cosine.",
  example = "> SELECT cos(x);",
  returnType = TajoDataTypes.Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4}),
          @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8})}
)
public class Cos extends GeneralFunction {
  public Cos() {
    super(new Column[] {
        new Column("x", TajoDataTypes.Type.FLOAT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    return DatumFactory.createFloat8(Math.cos(params.getFloat8(0)));
  }
}
