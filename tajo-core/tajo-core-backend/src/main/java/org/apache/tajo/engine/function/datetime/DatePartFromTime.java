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
    description = "Extract field from time",
    example = "> SELECT date_part('second', time '10:09:37.5');\n"
        + "37.5",
    returnType = TajoDataTypes.Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TIME})}
)
public class DatePartFromTime extends GeneralFunction {
  public DatePartFromTime() {
    super(new Column[] {
        new Column("target", FLOAT8),
        new Column("source", TEXT)
    });
  }

  private DatePartExtractorFromTime extractor = null;

  @Override
  public Datum eval(Tuple params) {
    Datum target = params.get(0);
    TimeDatum time = null;

    if(target instanceof NullDatum || params.get(1) instanceof NullDatum) {
      return NullDatum.get();
    }

    if(params.get(1) instanceof TimeDatum) {
      time = (TimeDatum)(params.get(1));
    } else {
      return NullDatum.get();
    }

    if (extractor == null) {
      String extractType = target.asChars().toLowerCase();

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
      } else if (extractType.equals("timezone")) {
        extractor = new NullExtractorFromTime();
      } else if (extractType.equals("timezone_hour")) {
        extractor = new NullExtractorFromTime();
      } else if (extractType.equals("timezone_minute")) {
        extractor = new NullExtractorFromTime();
      } else {
        extractor = new NullExtractorFromTime();
      }
    }

    return extractor.extract(time);
  }

  private interface DatePartExtractorFromTime {
    public Datum extract(TimeDatum time);
  }

  private class HourExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeDatum time) {
      return DatumFactory.createFloat8((double) time.getHourOfDay());
    }
  }

  private class MicrosecondsExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeDatum time) {
      return DatumFactory.createFloat8((double) (time.getSecondOfMinute() * 1000000 + time.getMillisOfSecond() * 1000));
    }
  }

  private class MillisecondsExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeDatum time) {
      return DatumFactory.createFloat8((double) (time.getSecondOfMinute() * 1000 + time.getMillisOfSecond()));
    }
  }

  private class MinuteExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeDatum time) {
      return DatumFactory.createFloat8((double) time.getMinuteOfHour());
    }
  }

  private class SecondExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeDatum time) {
      if (time.getMillisOfSecond() != 0) {
        return DatumFactory.createFloat8(time.getSecondOfMinute() + (((double) time.getMillisOfSecond()) / 1000));
      } else {
        return DatumFactory.createFloat8((double) time.getSecondOfMinute());
      }
    }
  }

  private class NullExtractorFromTime implements DatePartExtractorFromTime {
    @Override
    public Datum extract(TimeDatum time) {
      return NullDatum.get();
    }
  }
}