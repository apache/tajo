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
 * INT8 ceil(value FLOAT4/FLOAT8)
 */
@Description(
  functionName = "ceil",
  synonyms = {"ceiling"},
  description = "Smallest integer not less than argument.",
  example = "> SELECT ceil(-42.8);\n"
          + "-42",
  returnType = TajoDataTypes.Type.INT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4}),
          @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8})}
)
public class Ceil extends GeneralFunction {
  public Ceil() {
    super(new Column[] {
      new Column("x", TajoDataTypes.Type.FLOAT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    return DatumFactory.createInt8((long)Math.ceil(params.getFloat8(0)));
  }

}
