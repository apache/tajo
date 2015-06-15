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
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;
import static org.apache.tajo.common.TajoDataTypes.Type.TIMESTAMP;

@Description(
  functionName = "to_char",
  description = "Convert time stamp to string. Format should be a SQL standard format string.",
  example = "> SELECT to_char(TIMESTAMP '2014-01-17 10:09:37', 'YYYY-MM');\n"
          + "2014-01",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TIMESTAMP, TajoDataTypes.Type.TEXT})}
)
public class ToCharTimestamp extends GeneralFunction {
  @Expose private TimeZone timezone;

  public ToCharTimestamp() {
    super(new Column[] {
        new Column("timestamp", TIMESTAMP),
        new Column("format", TEXT)
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] paramTypes) {
    String timezoneId = context.get(SessionVars.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE);
    timezone = TimeZone.getTimeZone(timezoneId);
  }

  @Override
  public Datum eval(Tuple params) {
    if(params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    TimeMeta tm = params.getTimeDate(0);
    String pattern = params.getText(1);

    DateTimeUtil.toUserTimezone(tm, timezone);

    return DatumFactory.createText(DateTimeFormat.to_char(tm, pattern));
  }
}
