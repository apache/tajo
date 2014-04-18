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
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * Float8 acos(value FLOAT8)
 */
@Description(
  functionName = "acos",
  description = "Inverse cosine.",
  detail = "",
  example = "> SELECT acos(x);",
  returnType = TajoDataTypes.Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4}),
          @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8})}
)
public class Acos extends GeneralFunction {
  public Acos() {
    super(new Column[] {
        new Column("x", TajoDataTypes.Type.FLOAT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum valueDatum = params.get(0);
    if(valueDatum instanceof NullDatum) {
      return NullDatum.get();
    }

    return DatumFactory.createFloat8(Math.acos(valueDatum.asFloat8()));
  }
}
