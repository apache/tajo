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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.TimeDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

@Description(
    functionName = "current_time",
    description = "Get current time. Result is TIME type.",
    example = "> SELECT current_time();\n12:30:40",
    returnType = TajoDataTypes.Type.TIME,
    paramTypes = {@ParamTypes(paramTypes = {})}
)
public class CurrentTime extends GeneralFunction {
  TimeDatum datum;

  public CurrentTime() {
    super(NoArgs);
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType [] types) {
    if (!hasTimeZone()) {
      setTimeZone(TimeZone.getTimeZone(context.get(SessionVars.TIMEZONE)));
    }
  }

  @Override
  public Datum eval(Tuple params) {
    if (datum == null) {
      long julianTimestamp = DateTimeUtil.javaTimeToJulianTime(System.currentTimeMillis());
      TimeMeta tm = new TimeMeta();
      DateTimeUtil.toJulianTimeMeta(julianTimestamp, tm);
      DateTimeUtil.toUserTimezone(tm, getTimeZone());
      datum = DatumFactory.createTime(DateTimeUtil.toTime(tm));
    }
    return datum;
  }
}
