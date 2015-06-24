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
import org.apache.tajo.util.datetime.TimeMeta;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

@Description(
    functionName = "date_part",
    description = "Extract field from date",
    example = "> SELECT date_part('month', date '2014-01-17');\n"
        + "1.0",
    returnType = TajoDataTypes.Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.DATE})}
)
public class DatePartFromDate extends GeneralFunction {
  public DatePartFromDate() {
    super(new Column[] {
        new Column("target", FLOAT8),
        new Column("source", TEXT)
    });
  }

  private DatePartExtractorFromDate extractor = null;

  @Override
  public Datum eval(Tuple params) {

    if (params.isBlankOrNull(0) || params.isBlankOrNull(1) || params.type(1) != DATE) {
      return NullDatum.get();
    }

    TimeMeta timeMeta = params.getTimeDate(1);

    if (extractor == null) {
      String extractType = params.getText(0).toLowerCase();

      if (extractType.equals("century")) {
        extractor = new CenturyExtractorFromDate();
      } else if (extractType.equals("day")) {
        extractor = new DayExtractorFromDate();
      } else if (extractType.equals("decade")) {
        extractor = new DecadeExtractorFromDate();
      } else if (extractType.equals("dow")) {
        extractor = new DowExtractorFromDate();
      } else if (extractType.equals("doy")) {
        extractor = new DoyExtractorFromDate();
      } else if (extractType.equals("isodow")) {
        extractor = new ISODowExtractorFromDate();
      } else if (extractType.equals("isoyear")) {
        extractor = new ISOYearExtractorFromDate();
      } else if (extractType.equals("millennium")) {
        extractor = new MillenniumExtractorFromDate();
      } else if (extractType.equals("month")) {
        extractor = new MonthExtractorFromDate();
      } else if (extractType.equals("quarter")) {
        extractor = new QuarterExtractorFromDate();
      } else if (extractType.equals("week")) {
        extractor = new WeekExtractorFromDate();
      } else if (extractType.equals("year")) {
        extractor = new YearExtractorFromDate();
      } else {
        extractor = new NullExtractorFromDate();
      }
    }

    return extractor.extract(timeMeta);
  }

  private interface DatePartExtractorFromDate {
    public Datum extract(TimeMeta timeMeta);
  }

  private static class CenturyExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.getCenturyOfEra());
    }
  }

  private static class DayExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.dayOfMonth);
    }
  }

  private static class DecadeExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((timeMeta.years / 10.0d));
    }
  }

  private static class DowExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.getDayOfWeek());
    }
  }

  private static class DoyExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.getDayOfYear());
    }
  }

  private static class ISODowExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.getISODayOfWeek());
    }
  }

  private static class ISOYearExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.getWeekyear());
    }
  }

  private static class MillenniumExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) (((timeMeta.years - 1) / 1000) + 1));
    }
  }

  private static class MonthExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.monthOfYear);
    }
  }

  private static class QuarterExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) (((timeMeta.monthOfYear - 1) / 3) + 1));
    }
  }

  private static class WeekExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.getWeekOfYear());
    }
  }

  private static class YearExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return DatumFactory.createFloat8((double) timeMeta.years);
    }
  }

  private static class NullExtractorFromDate implements DatePartExtractorFromDate {
    @Override
    public Datum extract(TimeMeta timeMeta) {
      return NullDatum.get();
    }
  }
}