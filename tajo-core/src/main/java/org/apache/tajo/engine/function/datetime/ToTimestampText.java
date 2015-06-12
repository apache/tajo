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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

@Description(
    functionName = "to_timestamp",
    description = "Convert string to time stamp",
    detail = "Patterns for Date/Time Formatting: http://www.postgresql.org/docs/8.4/static/functions-formatting.html",
    example = "> select to_timestamp('05 Dec 2000 15:12:02.020', 'DD Mon YYYY HH24:MI:SS.MS');\n"
        + "2000-12-05 15:12:02.02",
    returnType = TajoDataTypes.Type.TIMESTAMP,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class ToTimestampText extends GeneralFunction {
  private TimeZone timezone;

  public ToTimestampText() {
    super(new Column[]{new Column("DateTimeText", TEXT), new Column("Pattern", TEXT)});
  }

  public void init(OverridableConf queryContext, FunctionEval.ParamType [] paramTypes) {
    String timezoneId = queryContext.get(SessionVars.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE);
    timezone = TimeZone.getTimeZone(timezoneId);
  }

  @Override
  public Datum eval(Tuple params) {
    if(params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    TimeMeta tm = DateTimeFormat.parseDateTime(params.getText(0), params.getText(1));
    DateTimeUtil.toUTCTimezone(tm, timezone);

    return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
  }
}
