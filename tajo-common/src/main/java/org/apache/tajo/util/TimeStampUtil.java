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
package org.apache.tajo.util;


import org.apache.tajo.datum.Int8Datum;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;

public class TimeStampUtil {

    public static long getDay(DateTime dateTime) {
        return convertToMicroSeconds(dateTime.withTimeAtStartOfDay());
    }

    public static long getHour(DateTime dateTime) {
        return convertToMicroSeconds(dateTime.withTime(dateTime.get(DateTimeFieldType.hourOfDay()), 0, 0, 0));
    }

    public static long getMinute(DateTime dateTime) {
        return convertToMicroSeconds(dateTime.withTime(dateTime.get(DateTimeFieldType.hourOfDay()),
            dateTime.get(DateTimeFieldType.minuteOfHour()), 0, 0));
    }

    public static long getSecond(DateTime dateTime) {
        return convertToMicroSeconds(dateTime.withTime(dateTime.get(DateTimeFieldType.hourOfDay()),
            dateTime.get(DateTimeFieldType.minuteOfHour()), dateTime.get(DateTimeFieldType.secondOfMinute()), 0));
    }

    public static long getMonth(DateTime dateTime) {
        return convertToMicroSeconds(dateTime.withTimeAtStartOfDay().withDate(dateTime.getYear(),
            dateTime.getMonthOfYear(),1));
    }

    public static long getDayOfWeek(DateTime dateTime,int week) {
        return convertToMicroSeconds(dateTime.withTimeAtStartOfDay().withDayOfWeek(week));
    }

    public static long getYear (DateTime dateTime) {
        return convertToMicroSeconds(dateTime.withTimeAtStartOfDay().withDate(dateTime.getYear(), 1, 1));
    }

    public static DateTime getUTCDateTime(Int8Datum int8Datum){
        return new DateTime(int8Datum.asInt8()/1000, DateTimeZone.UTC);
    }

    public static long convertToMicroSeconds(DateTime dateTime) {
        return  dateTime.getMillis() * 1000;
    }
}
