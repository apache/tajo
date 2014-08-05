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
 * INT8 round(value FLOAT8)
 */
@Description(
  functionName = "round",
  description = "Round to nearest integer.",
  example = "> SELECT round(42.4)\n"
          + "42",
  returnType = TajoDataTypes.Type.INT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8})
    }
)
public class Round extends GeneralFunction {
  public Round() {
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

    double value = valueDatum.asFloat8();

    // Note: there are various round up/down approaches (https://en.wikipedia.org/wiki/Rounding#Tie-breaking).
    //       Math.round uses an approach different from other programming languages, so the results of round function
    //       can be different from other DBMSs. For example, Math.round(-5.5) returns -5. In contrast,
    //       round function in MySQL and PostgreSQL returns -6. The below code is a workaround code for this.
    if (value < 0) {
      return DatumFactory.createInt8((long) Math.ceil(value - 0.5d));
    } else {
      return DatumFactory.createInt8((long) Math.floor(value + 0.5d));
    }
  }
}
