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
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.storage.Tuple;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Function definition
 *
 * INT8 round(value FLOAT8)
 */
@Description(
    functionName = "round",
    description = "Round to s decimalN places.",
    example = "> SELECT round(42.4382, 2)\n"
        + "42.44",
    returnType = TajoDataTypes.Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.INT4}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.INT4})}
)
public class RoundFloat8 extends GeneralFunction {
  public RoundFloat8() {
    super(new Column[] {
        new Column("value", TajoDataTypes.Type.FLOAT8),
        new Column("roundPoint", TajoDataTypes.Type.INT4)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum valueDatum = params.get(0);
    Datum roundDatum = params.get(1);

    if(valueDatum instanceof NullDatum || roundDatum instanceof  NullDatum) {
      return NullDatum.get();
    }

    double value = valueDatum.asFloat8();
    int rountPoint = roundDatum.asInt4();

    if (Double.isNaN(value)) {
      throw new InvalidOperationException("value is not a number.");
    }

    if (Double.isInfinite(value)) {
      throw new InvalidOperationException("value is infinite.");
    }

    try {
      return DatumFactory.createFloat8(BigDecimal.valueOf(value).setScale(rountPoint,
          RoundingMode.HALF_UP).doubleValue());
    } catch (Exception e) {
      throw new InvalidOperationException("RoundFloat8 eval error cause " + e.getMessage() + ", value=" + value +
          ", round point=" + rountPoint);
    }
  }
}