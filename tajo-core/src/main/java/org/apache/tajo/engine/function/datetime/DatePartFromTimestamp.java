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
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

@Description(
    functionName = "date_part",
    description = "Extract field from timestamp",
    example = "> SELECT date_part('year', timestamp '2014-01-17 10:09:37.5');\n"
        + "2014.0",
    returnType = TajoDataTypes.Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TIMESTAMP})}
)
public class DatePartFromTimestamp extends GeneralFunction {
  public DatePartFromTimestamp() {
    super(new Column[] {
        new Column("target", FLOAT8),
        new Column("source", TEXT)
    });
  }

  private DatePartExtractorFromTimestamp extractor = null;

  @Override
  public Datum eval(Tuple params) {
    Datum target = params.get(0);
    TimestampDatum timestamp = null;

    if(target instanceof NullDatum || params.get(1) instanceof NullDatum) {
      return NullDatum.get();
    }

    if(params.get(1) instanceof TimestampDatum) {
      timestamp = (TimestampDatum)(params.get(1));
    } else {
      return NullDatum.get();
    }

    if (extractor == null) {
      String extractType = target.asChars().toLowerCase();

      if (extractType.equals("century")) {
        extractor = new CenturyExtractorFromTimestamp();
      } else if (extractType.equals("day")) {
        extractor = new DayExtractorFromTimestamp();
      } else if (extractType.equals("decade")) {
        extractor = new DecadeExtractorFromTimestamp();
      } else if (extractType.equals("dow")) {
        extractor = new DowExtractorFromTimestamp();
      } else if (extractType.equals("doy")) {
        extractor = new DoyExtractorFromTimestamp();
      } else if (extractType.equals("epoch")) {
        extractor = new EpochExtractorFromTimestamp();
      } else if (extractType.equals("hour")) {
        extractor = new HourExtractorFromTimestamp();
      } else if (extractType.equals("isodow")) {
        extractor = new ISODowExtractorFromTimestamp();
      } else if (extractType.equals("isoyear")) {
        extractor = new ISOYearExtractorFromTimestamp();
      } else if (extractType.equals("microseconds")) {
        extractor = new MicrosecondsExtractorFromTimestamp();
      } else if (extractType.equals("millennium")) {
        extractor = new MillenniumExtractorFromTimestamp();
      } else if (extractType.equals("milliseconds")) {
        extractor = new MillisecondsExtractorFromTimestamp();
      } else if (extractType.equals("minute")) {
        extractor = new MinuteExtractorFromTimestamp();
      } else if (extractType.equals("month")) {
        extractor = new MonthExtractorFromTimestamp();
      } else if (extractType.equals("quarter")) {
        extractor = new QuarterExtractorFromTimestamp();
      } else if (extractType.equals("second")) {
        extractor = new SecondExtractorFromTimestamp();
      } else if (extractType.equals("timezone")) {
        extractor = new NullExtractorFromTimestamp();
      } else if (extractType.equals("timezone_hour")) {
        extractor = new NullExtractorFromTimestamp();
      } else if (extractType.equals("timezone_minute")) {
        extractor = new NullExtractorFromTimestamp();
      } else if (extractType.equals("week")) {
        extractor = new WeekExtractorFromTimestamp();
      } else if (extractType.equals("year")) {
        extractor = new YearExtractorFromTimestamp();
      } else {
        extractor = new NullExtractorFromTimestamp();
      }
    }

    return extractor.extract(timestamp);
  }

  private interface DatePartExtractorFromTimestamp {
    public Datum extract(TimestampDatum timestamp);
  }

  private class CenturyExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getCenturyOfEra());
    }
  } 

  private class DayExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getDayOfMonth());
    }
  }

  private class DecadeExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) (timestamp.getYear() / 10));
    }
  }

  private class DowExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      Integer tdow = timestamp.getDayOfWeek();
      return DatumFactory.createFloat8((double) ((tdow == 7) ? 0 : tdow));
    }
  }

  private class DoyExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getDayOfYear());
    }
  }

  private class EpochExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getUnixTime());
    }
  }

  private class HourExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getHourOfDay());
    }
  }

  private class ISODowExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getDayOfWeek());
    }
  }

  private class ISOYearExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getWeekyear());
    }
  }

  private class MicrosecondsExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) (timestamp.getSecondOfMinute() * 1000000 + timestamp.getMillisOfSecond() * 1000));
    }
  }

  private class MillenniumExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) (((timestamp.getYear() - 1) / 1000) + 1));
    }
  }

  private class MillisecondsExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) (timestamp.getSecondOfMinute() * 1000 + timestamp.getMillisOfSecond()));
    }
  }

  private class MinuteExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getMinuteOfHour());
    }
  }

  private class MonthExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getMonthOfYear());
    }
  }

  private class QuarterExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) (((timestamp.getMonthOfYear() - 1) / 3) + 1));
    }
  }

  private class SecondExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      if (timestamp.getMillisOfSecond() != 0) {
        return DatumFactory.createFloat8(timestamp.getSecondOfMinute() + (((double) timestamp.getMillisOfSecond()) / 1000));
      } else {
        return DatumFactory.createFloat8((double) timestamp.getSecondOfMinute());
      }
    }
  }

  private class WeekExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getWeekOfWeekyear());
    }
  }

  private class YearExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return DatumFactory.createFloat8((double) timestamp.getYear());
    }
  }

  private class NullExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimestampDatum timestamp) {
      return NullDatum.get();
    }
  }
}

