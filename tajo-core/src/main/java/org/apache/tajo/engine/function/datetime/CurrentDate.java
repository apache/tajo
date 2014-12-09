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

import com.google.gson.annotations.Expose;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DateDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

@Description(
    functionName = "current_date",
    description = "Get current date. Result is DATE type.",
    example = "> SELECT current_date();\n2014-04-18",
    returnType = TajoDataTypes.Type.DATE,
    paramTypes = {@ParamTypes(paramTypes = {})}
)
public class CurrentDate extends GeneralFunction {
  @Expose private TimeZone timezone;
  private DateDatum datum;

  public CurrentDate() {
    super(NoArgs);
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] types) {
    String timezoneId = context.get(SessionVars.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE);
    timezone = TimeZone.getTimeZone(timezoneId);
  }

  @Override
  public Datum eval(Tuple params) {
    if (datum == null) {
      long julianTimestamp = DateTimeUtil.javaTimeToJulianTime(System.currentTimeMillis());
      TimeMeta tm = new TimeMeta();
      DateTimeUtil.toJulianTimeMeta(julianTimestamp, tm);
      DateTimeUtil.toUserTimezone(tm, timezone);
      datum = DatumFactory.createDate(tm.years, tm.monthOfYear, tm.dayOfMonth);
    }
    return datum;
  }
}
