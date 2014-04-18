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
import org.apache.tajo.util.TimeStampUtil;
import org.joda.time.DateTime;

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
public class DateTimePartFromUnixTimeStamp extends GeneralFunction {

    private DateTimePartExtractorFromUnixTime extractor = null;
    private WeekPartExtractorFromUnixTime weekExtractor = null;

    public DateTimePartFromUnixTimeStamp() {
        super(new Column[]{
                new Column("target", TEXT),
                new Column("source", INT8),
                new Column("dayOfWeek", INT4),

        });
    }

    @Override
    public Datum eval(Tuple params) {

        Datum target = params.get(0);
        DateTime dateTime;
        Int4Datum dayOfWeek = null;

        if (target instanceof NullDatum || params.get(1) instanceof NullDatum) {
            return NullDatum.get();
        }

        if (params.get(1) instanceof Int8Datum) {
            dateTime = TimeStampUtil.getUTCDateTime((Int8Datum) (params.get(1)));
        } else {
            return NullDatum.get();
        }


        if ( null == extractor || null == weekExtractor) {

            String extractType = target.asChars().toLowerCase();

            if (extractType.equals("day")) {
                extractor = new DayExtractorFromTime();
            } else if (extractType.equals("hour")) {
                extractor = new HourExtractorFromTime();
            } else if (extractType.equals("month")) {
                extractor = new MonthExtractorFromTime();
            } else if (extractType.equals("year")) {
                extractor = new YearExtractorFromTime();
            } else if (extractType.equals("week")) {
                if (params.get(2) instanceof NullDatum) {
                    return NullDatum.get();
                }
                dayOfWeek = (Int4Datum) params.get(2);
                weekExtractor = new WeekExtractorFromTime();
            }
        }

        return null != weekExtractor ? weekExtractor.extract(dateTime, dayOfWeek.asInt4()) : extractor.extract(dateTime);
    }

    private interface DateTimePartExtractorFromUnixTime {
        public Datum extract(DateTime dateTime);
    }

    private interface WeekPartExtractorFromUnixTime {
        public Datum extract(DateTime dateTime, int week);
    }

    private class DayExtractorFromTime implements DateTimePartExtractorFromUnixTime {
        @Override
        public Datum extract(DateTime dateTime) {
            return DatumFactory.createInt8(TimeStampUtil.getDay(dateTime));
        }
    }

    private class HourExtractorFromTime implements DateTimePartExtractorFromUnixTime {
        @Override
        public Datum extract(DateTime dateTime) {
            return DatumFactory.createInt8(TimeStampUtil.getHour(dateTime));
        }
    }

    private class MonthExtractorFromTime implements DateTimePartExtractorFromUnixTime {
        @Override
        public Datum extract(DateTime dateTime) {
            return DatumFactory.createInt8(TimeStampUtil.getMonth(dateTime));
        }
    }

    private class YearExtractorFromTime implements DateTimePartExtractorFromUnixTime {
        @Override
        public Datum extract(DateTime dateTime) {
            return DatumFactory.createInt8(TimeStampUtil.getYear(dateTime));
        }
    }

    private class WeekExtractorFromTime implements WeekPartExtractorFromUnixTime {
        @Override
        public Datum extract(DateTime dateTime , int week) {
            return DatumFactory.createInt8(TimeStampUtil.getDayOfWeek(dateTime,week));
        }
    }
}
