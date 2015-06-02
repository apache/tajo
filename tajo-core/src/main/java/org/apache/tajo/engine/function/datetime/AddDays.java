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

package org.apache.tajo.engine.function.datetime;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "add_days",
    description = "Return date value which is added with given parameter.",
    example = "> SELECT add_days(date '2013-12-30', 5);\n"
        + "2014-01-04 00:00:00",
    returnType = Type.TIMESTAMP,
    paramTypes = {
        @ParamTypes(paramTypes = {Type.DATE, Type.INT2}),
        @ParamTypes(paramTypes = {Type.DATE, Type.INT4}),
        @ParamTypes(paramTypes = {Type.DATE, Type.INT8}),
        @ParamTypes(paramTypes = {Type.TIMESTAMP, Type.INT2}),
        @ParamTypes(paramTypes = {Type.TIMESTAMP, Type.INT4}),
        @ParamTypes(paramTypes = {Type.TIMESTAMP, Type.INT8})
    }
)
public class AddDays extends GeneralFunction {
  public AddDays() {
    super(new Column[]{
        new Column("date", TajoDataTypes.Type.DATE),
        new Column("day", TajoDataTypes.Type.INT4)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum dateDatum = params.asDatum(0);
    long val = params.getInt8(1);
    if (val >= 0) {
      return dateDatum.plus(new IntervalDatum(val * IntervalDatum.DAY_MILLIS));
    } else {
      return dateDatum.minus(new IntervalDatum(0 - val * IntervalDatum.DAY_MILLIS));
    }
  }
}

