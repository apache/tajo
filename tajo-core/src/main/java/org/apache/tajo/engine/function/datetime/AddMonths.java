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

import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

import java.util.TimeZone;

@Description(
    functionName = "add_months",
    description = "Return date value which is added with given parameter.",
    example = "> SELECT add_months(date '2013-12-17', 2);\n"
        + "2014-02-17 00:00:00",
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
public class AddMonths extends GeneralFunction {

  public AddMonths() {
    super(new Column[]{
        new Column("date", TajoDataTypes.Type.DATE),
        new Column("month", TajoDataTypes.Type.INT4)
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] types) {
    setTimeZone(TimeZone.getTimeZone(context.get(SessionVars.TIMEZONE)));
  }

  @Override
  public Datum eval(Tuple params) {
    // cast to UTC timestamp
    Datum dateDatum = DatumFactory.createTimestamp(params.asDatum(0), getTimeZone());

    int val = params.getInt4(1);
    if (val >= 0) {
      return dateDatum.plus(new IntervalDatum(val, 0));
    } else {
      return dateDatum.minus(new IntervalDatum(-val, 0));
    }
  }
}
