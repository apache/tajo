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
import org.apache.tajo.datum.*;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import static org.apache.tajo.common.TajoDataTypes.Type.*;


@Description(
    functionName = "utc_usec_to",
    description = "Extract field from time",
    example = "> SELECT utc_usec_to('day', 1274259481071200);\n"
        + "1274227200000000",
    returnType = TajoDataTypes.Type.INT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT8}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT8, TajoDataTypes.Type.INT4})}
)
public class DateTimePartFromUnixTimestamp extends GeneralFunction {

  private DateTimePartExtractorFromUnixTime extractor = null;
  private WeekPartExtractorFromUnixTime weekExtractor = null;

  public DateTimePartFromUnixTimestamp() {
    super(new Column[]{
        new Column("target", TEXT),
        new Column("source", INT8),
        new Column("dayOfWeek", INT4),

    });
  }

  @Override
  public Datum eval(Tuple params) {

    if (params.isBlankOrNull(0) || params.isBlankOrNull(1) || params.type(1) != INT8) {
      return NullDatum.get();
    }

    TimeMeta dateTime = DateTimeUtil.getUTCDateTime(params.getInt8(1));


    if (extractor == null && weekExtractor == null) {

      String extractType = params.getText(0).toLowerCase();

      if (extractType.equals("day")) {
        extractor = new DayExtractorFromTime();
      } else if (extractType.equals("hour")) {
        extractor = new HourExtractorFromTime();
      } else if (extractType.equals("month")) {
        extractor = new MonthExtractorFromTime();
      } else if (extractType.equals("year")) {
        extractor = new YearExtractorFromTime();
      } else if (extractType.equals("week")) {
        weekExtractor = new WeekExtractorFromTime();
      }
    }

    if (extractor != null) {
      return extractor.extract(dateTime);
    }

    return params.isBlankOrNull(2) ? NullDatum.get() : weekExtractor.extract(dateTime, params.getInt4(2));
  }

  private interface DateTimePartExtractorFromUnixTime {
    public Datum extract(TimeMeta dateTime);
  }

  private interface WeekPartExtractorFromUnixTime {
    public Datum extract(TimeMeta dateTime, int week);
  }

  private static class DayExtractorFromTime implements DateTimePartExtractorFromUnixTime {
    @Override
    public Datum extract(TimeMeta dateTime) {
      return DatumFactory.createInt8(DateTimeUtil.getDay(dateTime));
    }
  }

  private static class HourExtractorFromTime implements DateTimePartExtractorFromUnixTime {
    @Override
    public Datum extract(TimeMeta dateTime) {
      return DatumFactory.createInt8(DateTimeUtil.getHour(dateTime));
    }
  }

  private static class MonthExtractorFromTime implements DateTimePartExtractorFromUnixTime {
    @Override
    public Datum extract(TimeMeta dateTime) {
      return DatumFactory.createInt8(DateTimeUtil.getMonth(dateTime));
    }
  }

  private static class YearExtractorFromTime implements DateTimePartExtractorFromUnixTime {
    @Override
    public Datum extract(TimeMeta dateTime) {
      return DatumFactory.createInt8(DateTimeUtil.getYear(dateTime));
    }
  }

  private static class WeekExtractorFromTime implements WeekPartExtractorFromUnixTime {
    @Override
    public Datum extract(TimeMeta dateTime , int week) {
      return DatumFactory.createInt8(DateTimeUtil.getDayOfWeek(dateTime,week));
    }
  }
}
