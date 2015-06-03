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
import org.apache.tajo.util.datetime.DateTimeConstants;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

import static org.apache.tajo.common.TajoDataTypes.Type.TIME;
import static org.apache.tajo.common.TajoDataTypes.Type.FLOAT8;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

@Description(
    functionName = "date_part",
    description = "Extract field from time",
    example = "> SELECT date_part('second', time '10:09:37.5');\n"
        + "37.5",
    returnType = TajoDataTypes.Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TIME})}
)
public class DatePartFromTime extends GeneralFunction {
  @Expose  private TimeZone timezone;
  private DatePartExtractorFromTime extractor = null;

  public DatePartFromTime() {
    super(new Column[] {
        new Column("target", FLOAT8),
        new Column("source", TEXT)
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType [] types) {
    String timezoneId = context.get(SessionVars.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE);
    timezone = TimeZone.getTimeZone(timezoneId);
  }

  @Override
  public Datum eval(Tuple params) {

    if (params.isBlankOrNull(0) || params.isBlankOrNull(1) || params.type(1) != TIME) {
      return NullDatum.get();
    }

    if (extractor == null) {
      String extractType = params.getText(0).toLowerCase();

      if (extractType.equals("hour")) {
        extractor = new HourExtractorFromTime();
      } else if (extractType.equals("microseconds")) {
        extractor = new MicrosecondsExtractorFromTime();
      } else if (extractType.equals("milliseconds")) {
        extractor = new MillisecondsExtractorFromTime();
      } else if (extractType.equals("minute")) {
        extractor = new MinuteExtractorFromTime();
      } else if (extractType.equals("second")) {
        extractor = new SecondExtractorFromTime();
      } else {
        extractor = new NullExtractorFromTime();
      }
    }

    TimeMeta tm = params.getTimeDate(1);
    DateTimeUtil.toUserTimezone(tm, timezone);
    return extractor.extract(tm);
  }

  private interface DatePartExtractorFromTime {
    public Datum extract(TimeMeta tm);
  }

  private static class HourExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.hours);
    }
  }

  private static class MicrosecondsExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (tm.secs * 1000000 + tm.fsecs));
    }
  }

  private static class MillisecondsExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (tm.secs * 1000 + tm.fsecs / 1000.0));
    }
  }

  private static class MinuteExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.minutes);
    }
  }

  private static class SecondExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeMeta tm) {
      if (tm.fsecs != 0) {
        return DatumFactory.createFloat8(tm.secs + (((double) tm.fsecs) / (double)DateTimeConstants.USECS_PER_SEC));
      } else {
        return DatumFactory.createFloat8((double) tm.secs);
      }
    }
  }

  private static class NullExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeMeta tm) {
      return NullDatum.get();
    }
  }
}