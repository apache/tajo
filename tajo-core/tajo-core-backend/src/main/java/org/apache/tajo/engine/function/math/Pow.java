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
 * Float8 pow(value FLOAT8, value FLOAT8)
 */
@Description(
  functionName = "pow",
  description = "x raised to the power of y",
  example = "> SELECT pow(9.0, 3.0)\n"
           + "729",
  returnType = TajoDataTypes.Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.FLOAT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.FLOAT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.FLOAT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.FLOAT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.INT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.INT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.INT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.INT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.FLOAT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.FLOAT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.FLOAT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.FLOAT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.INT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.INT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.INT4}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.INT8})
  }
)
public class Pow extends GeneralFunction {
  public Pow() {
    super(new Column[] {
        new Column("x", TajoDataTypes.Type.FLOAT8),
        new Column("y", TajoDataTypes.Type.FLOAT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum value1Datum = params.get(0);
    Datum value2Datum = params.get(1);
    if(value1Datum instanceof NullDatum || value2Datum instanceof NullDatum) {
      return NullDatum.get();
    }

    return DatumFactory.createFloat8(Math.pow(value1Datum.asFloat8(), value2Datum.asFloat8()));
  }
}
