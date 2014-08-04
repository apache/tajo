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

package org.apache.tajo.datum;

import com.google.common.base.Objects;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class IntervalDatum extends Datum {
  public static final long MINUTE_MILLIS = 60 * 1000;
  public static final long HOUR_MILLIS = 60 * MINUTE_MILLIS;
  public static final long DAY_MILLIS = 24 * HOUR_MILLIS;
  public static final long MONTH_MILLIS = 30 * DAY_MILLIS;

  static enum DATE_UNIT {
    CENTURY, DECADE, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MICROSEC, MILLISEC, TIMEZONE,
  }
  static Map<String, DATE_UNIT> DATE_FORMAT_LITERAL = new HashMap<String, DATE_UNIT>();
  static {
    Object[][] dateFormatLiterals = new Object[][]{
        {DATE_UNIT.CENTURY, "c,cent,centuries,century"},
        {DATE_UNIT.DAY, "d,day,days"},
        {DATE_UNIT.DECADE, "dec,decade,decades,decs"},
        {DATE_UNIT.HOUR, "h,hour,hours,hr,hrs"},
        {DATE_UNIT.MILLISEC, "millisecon,ms,msec,msecond,mseconds,msecs"},
        {DATE_UNIT.MINUTE, "m,min,mins,minute,minutes"},
        {DATE_UNIT.MONTH, "mon,mons,month,months"},
        {DATE_UNIT.SECOND, "s,sec,second,seconds,secs"},
        {DATE_UNIT.TIMEZONE, "timezone"},
        {DATE_UNIT.MICROSEC, "microsecon,us,usec,microsecond,useconds,usecs"},
        {DATE_UNIT.YEAR, "y,year,years,yr,yrs"}
    };

    for (Object[] eachLiteral: dateFormatLiterals) {
      String[] tokens = ((String)eachLiteral[1]).split(",");
      DATE_UNIT unit = (DATE_UNIT)eachLiteral[0];
      for (String eachToken: tokens) {
        DATE_FORMAT_LITERAL.put(eachToken, unit);
      }
    }
  }

  private int months;
  private long millieconds;

  public IntervalDatum(long milliseconds) {
    this(0, milliseconds);
  }

  public IntervalDatum(int months, long milliseconds) {
    super(TajoDataTypes.Type.INTERVAL);
    this.months = months;
    this.millieconds = milliseconds;
  }

  public IntervalDatum(String intervalStr) {
    super(TajoDataTypes.Type.INTERVAL);

    intervalStr = intervalStr.trim();
    if (intervalStr.isEmpty()) {
      throw new InvalidOperationException("interval expression is empty");
    }

    try {
      int year = 0;
      int month = 0;
      int day = 0;
      int hour = 0;
      int minute = 0;
      int second = 0;
      int microsecond = 0;
      int millisecond = 0;
      long time = 0;

      int length = intervalStr.getBytes().length;

      StringBuilder digitChars = new StringBuilder();
      StringBuilder unitChars = new StringBuilder();
      for (int i = 0; i < length; i++) {
        char c = intervalStr.charAt(i);
        if (Character.isDigit(c) || c == ':' || c == '.' || c == '-') {
          digitChars.append(c);
        } else if (c == ' ') {
          if (digitChars.length() > 0) {
            if (unitChars.length() > 0) {
              String unitName = unitChars.toString();
              DATE_UNIT foundUnit = DATE_FORMAT_LITERAL.get(unitName);
              if (foundUnit == null) {
                throw new InvalidOperationException("invalid input syntax for type interval: " + intervalStr);
              }
              int digit = Integer.parseInt(digitChars.toString());
              switch(foundUnit) {
                case YEAR:    year = digit;   break;
                case MONTH:   month = digit;  break;
                case DAY:     day = digit;    break;
                case HOUR:    hour = digit;   break;
                case MINUTE:  minute = digit; break;
                case SECOND:  second = digit;   break;
                case MICROSEC:  microsecond = digit;  break;
                case MILLISEC:  millisecond = digit;  break;
              default: throw new InvalidOperationException("Unknown datetime unit: " + foundUnit);
              }
              digitChars.setLength(0);
              unitChars.setLength(0);
            } else if (digitChars.indexOf(":") >= 0) {
              time = parseTime(digitChars.toString());
              digitChars.setLength(0);
            }
          }
        } else {
          unitChars.append(c);
        }
      }
      if (digitChars.length() > 0) {
        if (unitChars.length() > 0) {
          String unitName = unitChars.toString();
          DATE_UNIT foundUnit = DATE_FORMAT_LITERAL.get(unitName);
          if (foundUnit == null) {
            throw new InvalidOperationException("invalid input syntax for type interval: " + intervalStr);
          }
          int digit = Integer.parseInt(digitChars.toString());
          switch(foundUnit) {
            case YEAR:    year = digit;   break;
            case MONTH:   month = digit;  break;
            case DAY:     day = digit;    break;
            case HOUR:    hour = digit;   break;
            case MINUTE:  minute = digit; break;
            case SECOND:  second = digit;   break;
            case MICROSEC:  microsecond = digit;  break;
            case MILLISEC:  millisecond = digit;  break;
            default: throw new InvalidOperationException("Unknown datetime unit: " + foundUnit);
          }
        } else if (digitChars.indexOf(":") >= 0) {
          time = parseTime(digitChars.toString());
          digitChars.setLength(0);
        }
      }

      if (time > 0 && (hour != 0 || minute != 0 || second != 0 || microsecond != 0 || millisecond != 0)) {
          throw new InvalidOperationException("invalid input syntax for type interval: " + intervalStr);
      }

      this.millieconds = time + day * DAY_MILLIS + hour * HOUR_MILLIS + minute * 60 * 1000L + second * 1000L +
          microsecond * 100L + millisecond;
      this.months = year * 12 + month;
    } catch (InvalidOperationException e) {
      throw e;
    } catch (Throwable t) {
      throw new InvalidOperationException(t.getMessage() + ": " + intervalStr);
    }
  }

  public static long parseTime(String timeStr) {
    //parse HH:mm:ss.SSS
    int hour = 0;
    int minute = 0;

    int second = 0;
    int millisecond = 0;
    String[] timeTokens = timeStr.split(":");
    if (timeTokens.length == 1) {
      //sec
      String[] secondTokens = timeTokens[0].split("\\.");
      if (secondTokens.length == 1) {
        second = Integer.parseInt(secondTokens[0]);
      } else if (secondTokens.length == 2) {
        millisecond = Integer.parseInt(secondTokens[1]);
      } else {
        throw new InvalidOperationException("invalid input syntax for type interval: " + timeStr);
      }
    } else {
      if (timeTokens.length > 3) {
        throw new InvalidOperationException("invalid input syntax for type interval: " + timeStr);
      }
      for(int i = 0; i < timeTokens.length - 1; i++) {
        if (i == 0)   hour = Integer.parseInt(timeTokens[i]);
        if (i == 1)   minute = Integer.parseInt(timeTokens[i]);
      }
      if (timeTokens.length == 3) {
        //sec
        String[] secondTokens = timeTokens[2].split("\\.");
        if (secondTokens.length == 1) {
          second = Integer.parseInt(secondTokens[0]);
        } else if (secondTokens.length == 2) {
          second = Integer.parseInt(secondTokens[0]);
          millisecond = Integer.parseInt(secondTokens[1]);
        } else {
          throw new InvalidOperationException("invalid input syntax for type interval: " + timeStr);
        }
      }
    }

    return hour * HOUR_MILLIS+ minute * MINUTE_MILLIS + second * 1000 + millisecond;
  }

  public int getMonths() {
    return this.months;
  }

  public long getMilliSeconds() {
    return millieconds;
  }

  @Override
  public Datum plus(Datum datum) {
    switch(datum.type()) {
      case INTERVAL:
        IntervalDatum other = (IntervalDatum) datum;
        return new IntervalDatum(months + other.months, millieconds + other.millieconds);
      case DATE: {
        DateDatum dateDatum = (DateDatum) datum;
        TimeMeta tm = dateDatum.toTimeMeta();
        tm.plusMillis(getMilliSeconds());
        if (getMonths() > 0) {
          tm.plusMonths(getMonths());
        }
        DateTimeUtil.toUTCTimezone(tm);
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      }
      case TIME: {
        TimeMeta tm = ((TimeDatum) datum).toTimeMeta();
        tm.plusMillis(millieconds);
        return new TimeDatum(DateTimeUtil.toTime(tm));
      }
      case TIMESTAMP: {
        TimeMeta tm = new TimeMeta();
        DateTimeUtil.toJulianTimeMeta(((TimestampDatum) datum).asInt8(), tm);
        if (months > 0) {
          tm.plusMonths(months);
        }
        if (millieconds > 0) {
          tm.plusMillis(millieconds);
        }
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
      }
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum minus(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.INTERVAL) {
      IntervalDatum other = (IntervalDatum) datum;
      return new IntervalDatum(months - other.months, millieconds - other.millieconds);
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum multiply(Datum datum) {
    switch(datum.type()) {
      case INT2:
      case INT4:
      case INT8:
        long int8Val = datum.asInt8();
        return createIntervalDatum((double)months * int8Val, (double) millieconds * int8Val);
      case FLOAT4:
      case FLOAT8:
        double float8Val = datum.asFloat8();
        return createIntervalDatum((double)months * float8Val, (double) millieconds * float8Val);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum divide(Datum datum) {
    switch (datum.type()) {
      case INT2:
      case INT4:
      case INT8:
        long paramValueI8 = datum.asInt8();
        if (!validateDivideZero(paramValueI8)) {
          return NullDatum.get();
        }
        return createIntervalDatum((double) months / paramValueI8, (double) millieconds / paramValueI8);
      case FLOAT4:
      case FLOAT8:
        double paramValueF8 = datum.asFloat8();
        if (!validateDivideZero(paramValueF8)) {
          return NullDatum.get();
        }
        return createIntervalDatum((double) months / paramValueF8, (double) millieconds / paramValueF8);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  private IntervalDatum createIntervalDatum(double monthValue, double millisValue) {
    int month = (int)(monthValue);
    return new IntervalDatum(month, Math.round((monthValue - (double)month) * (double)MONTH_MILLIS + (double)millisValue));
  }

  @Override
  public long asInt8() {
    return (months * 30) * DAY_MILLIS + millieconds;
  }

  public String toString() {
    return asChars();
  }

  static DecimalFormat df = new DecimalFormat("00");
  static DecimalFormat df2 = new DecimalFormat("000");
  @Override
  public String asChars() {
    try {
      StringBuilder sb = new StringBuilder();

      String prefix = "";

      if (months != 0) {
        int positiveNum = Math.abs(months);
        int year = positiveNum / 12;
        int remainMonth = positiveNum - year * 12;

        if (year > 0) {
          sb.append(months < 0 ? "-" : "");
          sb.append(year).append(year == 1 ? " year" : " years");
          prefix = " ";
        }
        sb.append(prefix).append(months < 0 ? "-" : "").append(remainMonth).append(months == 1 ? " month" : " months");
        prefix = " ";
      }

      formatMillis(sb, prefix, millieconds);
      return sb.toString();
    } catch (Exception e) {
      return "";
    }
  }

  public static String formatMillis(long millis) {
    StringBuilder sb = new StringBuilder();
    formatMillis(sb, "", millis);
    return sb.toString();
  }

  public static void formatMillis(StringBuilder sb, String prefix, long millis) {
    if (millis != 0) {
      long positiveNum = Math.abs(millis);
      int days = (int)(positiveNum / DAY_MILLIS);
      long remainInterval = positiveNum - days * DAY_MILLIS;

      if(days != 0) {
        sb.append(prefix).append(millis < 0 ? "-" : "").append(days).append(days == 1 ? " day" : " days");
        prefix = " ";
      }
      if (remainInterval != 0) {
        int hour = (int) (remainInterval / HOUR_MILLIS);
        int minutes = (int) (remainInterval - hour * HOUR_MILLIS) / (60 * 1000);
        long sec = (int) (remainInterval - hour * HOUR_MILLIS - minutes * (60 * 1000L)) / 1000L;
        long millisecond = (int) (remainInterval - hour * HOUR_MILLIS - minutes * (60 * 1000L) - sec * 1000L);

        sb.append(prefix)
            .append(millis < 0 ? "-" : "")
            .append(df.format(hour))
            .append(":")
            .append(df.format(minutes))
            .append(":")
            .append(df.format(sec));

        if (millisecond > 0) {
          sb.append(".").append(df2.format(millisecond));
        }
      }
    }
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.INTERVAL) {
      long val = asInt8();
      long another = datum.asInt8();
      if (val < another) {
        return -1;
      } else if (val > another) {
        return 1;
      } else {
        return 0;
      }
    } else if (datum instanceof NullDatum || datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.INTERVAL) {
      return DatumFactory.createBool(asInt8() == datum.asInt8());
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IntervalDatum) {
      return asInt8() == ((IntervalDatum)obj).asInt8();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode(){
    return Objects.hashCode(asInt8());
  }
}
