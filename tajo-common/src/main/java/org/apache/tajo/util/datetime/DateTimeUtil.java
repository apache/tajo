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

package org.apache.tajo.util.datetime;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.exception.ValueOutOfRangeException;
import org.apache.tajo.util.datetime.DateTimeConstants.DateStyle;
import org.apache.tajo.util.datetime.DateTimeConstants.DateToken;
import org.apache.tajo.util.datetime.DateTimeConstants.TokenField;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Class is originated from j2date in datetime.c of PostgreSQL.
 */
public class DateTimeUtil {

  private static int MAX_FRACTION_LENGTH = 6;

  /** maximum possible number of fields in a date * string */
  private static int MAXDATEFIELDS = 25;

  public static boolean isJulianCalendar(int year, int month, int day) {
    return year <= 1752 && month <= 9 && day < 14;
  }

  public static int getCenturyOfEra(int year) {
    if (year > 0) {
      return (year - 1) / 100 + 1;
    } else if (year < 0) {
      //600BC to 501BC -> -6
      int pYear = -year;
      return -((pYear - 1) / 100 + 1);
    } else {
      return 0;
    }
  }

  public static boolean isLeapYear(int year) {
    return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
  }

  public static int getDaysInYearMonth(int year, int month) {
    if (isLeapYear(year)) {
      return DateTimeConstants.DAY_OF_MONTH[1][month - 1];
    } else {
      return DateTimeConstants.DAY_OF_MONTH[0][month - 1];
    }
  }

  /**
   * Julian date support.
   *
   * isValidJulianDate checks the minimum date exactly, but is a bit sloppy
   * about the maximum, since it's far enough out to not be especially
   * interesting.
   * @param years
   * @param months
   * @param days
   * @return
   */
  public static boolean isValidJulianDate(int years, int months, int days) {
    return years > DateTimeConstants.JULIAN_MINYEAR || years == DateTimeConstants.JULIAN_MINYEAR &&
        months > DateTimeConstants.JULIAN_MINMONTH || months == DateTimeConstants.JULIAN_MINMONTH &&
        days >= DateTimeConstants.JULIAN_MINDAY && years < DateTimeConstants.JULIAN_MAXYEAR;
  }

  /**
   * Calendar time to Julian date conversions.
   * Julian date is commonly used in astronomical applications,
   *	since it is numerically accurate and computationally simple.
   * The algorithms here will accurately convert between Julian day
   *	and calendar date for all non-negative Julian days_full
   *	(i.e. from Nov 24, -4713 on).
   *
   * These routines will be used by other date/time packages
   * - thomas 97/02/25
   *
   * Rewritten to eliminate overflow problems. This now allows the
   * routines to work correctly for all Julian day counts from
   * 0 to 2147483647	(Nov 24, -4713 to Jun 3, 5874898) assuming
   * a 32-bit integer. Longer types should also work to the limits
   * of their precision.
   * @param year
   * @param month
   * @param day
   * @return
   */
  public static int date2j(int year, int month, int day) {
    int julian;
    int century;

    if (month > 2) {
      month += 1;
      year += 4800;
    } else {
      month += 13;
      year += 4799;
    }

    century = year / 100;
    julian = (year * 365) - 32167;
    julian += (((year / 4) - century) + (century / 4));
    julian += ((7834 * month) / 256) + day;

    return julian;
  }

  /**
   * Set TimeMeta's date fields.
   * @param julianDate
   * @param tm
   */
  public static void j2date(int julianDate, TimeMeta tm) {
    long julian;
    long quad;
    long extra;
    long y;

    julian = julianDate;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
        + 123;
    y += quad * 4;


    tm.years = (int)(y - 4800);
    quad = julian * 2141 / 65536;
    tm.dayOfMonth = (int)(julian - 7834 * quad / 256);
    tm.monthOfYear = (int) ((quad + 10) % DateTimeConstants.MONTHS_PER_YEAR + 1);
  }

  /**
   * This method is originated from j2date in datetime.c of PostgreSQL.
   *
   * julianToDay - convert Julian date to day-of-week (0..6 == Sun..Sat)
   *
   * Note: various places use the locution julianToDay(date - 1) to produce a
   * result according to the convention 0..6 = Mon..Sun.	This is a bit of
   * a crock, but will work as long as the computation here is just a modulo.
   * @param julianDate
   * @return
   */
  public static int j2day(int julianDate) {
    long day;

    day = julianDate;

    day += 1;
    day %= 7;

    return (int) day;
  }

  /**
   * This method is originated from date2isoweek in timestamp.c of PostgreSQL.
   * Returns ISO week number of year.
   * @param year
   * @param mon
   * @param mday
   * @return
   */
  public static int date2isoweek(int year, int mon, int mday) {
    double result;
    int day0;
    int day4;
    int dayn;

    /* current day */
    dayn = date2j(year, mon, mday);

    /* fourth day of current year */
    day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    day0 = j2day(day4 - 1);

    /*
     * We need the first week containing a Thursday, otherwise this day falls
     * into the previous year for purposes of counting weeks
     */
    if (dayn < day4 - day0) {
      day4 = date2j(year - 1, 1, 4);

      /* day0 == offset to first day of week (Monday) */
      day0 = j2day(day4 - 1);
    }

    result = (dayn - (day4 - day0)) / 7 + 1;

      /*
       * Sometimes the last few days_full in a year will fall into the first week of
       * the next year, so check for this.
       */
    if (result >= 52) {
      day4 = date2j(year + 1, 1, 4);

      /* day0 == offset to first day of week (Monday) */
      day0 = j2day(day4 - 1);

      if (dayn >= day4 - day0) {
        result = (dayn - (day4 - day0)) / 7 + 1;
      }
    }

    return (int) result;
  }

  /**
   * date2isoyear()
   *
   * Returns ISO 8601 year number.
   * @param year
   * @param mon
   * @param mday
   * @return
   */
  public static int date2isoyear(int year, int mon, int mday) {
    /* current day */
    int dayn = date2j(year, mon, mday);

	  /* fourth day of current year */
    int day4 = date2j(year, 1, 4);

	  /* day0 == offset to first day of week (Monday) */
    int day0 = j2day(day4 - 1);

    /*
     * We need the first week containing a Thursday, otherwise this day falls
     * into the previous year for purposes of counting weeks
     */
    if (dayn < day4 - day0) {
      day4 = date2j(year - 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
      day0 = j2day(day4 - 1);

      year--;
    }

    double result = (dayn - (day4 - day0)) / 7 + 1;

    /*
     * Sometimes the last few days in a year will fall into the first week of
     * the next year, so check for this.
     */
    if (result >= 52) {
      day4 = date2j(year + 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
      day0 = j2day(day4 - 1);

      if (dayn >= day4 - day0) {
        year++;
      }
    }

    return year;
  }

  /**
   * Converts julian timestamp to epoch.
   * @param timestamp
   * @return
   */
  public static int julianTimeToEpoch(long timestamp) {
    long totalSecs = timestamp / DateTimeConstants.USECS_PER_SEC;
    return (int)(totalSecs + DateTimeConstants.SECS_DIFFERENCE_BETWEEN_JULIAN_AND_UNIXTIME);
  }

  /**
   * Converts julian timestamp to java timestamp.
   * @param timestamp
   * @return
   */
  public static long julianTimeToJavaTime(long timestamp) {
    double totalSecs = (double)timestamp / (double)DateTimeConstants.MSECS_PER_SEC;
    return (long)(Math.round(totalSecs + DateTimeConstants.SECS_DIFFERENCE_BETWEEN_JULIAN_AND_UNIXTIME * 1000.0));
  }

  /**
   * Converts java timestamp to julian timestamp.
   * @param javaTimestamp
   * @return
   */
  public static long javaTimeToJulianTime(long javaTimestamp) {
    double totalSecs = javaTimestamp / 1000.0;
    return (long)((totalSecs -
        DateTimeConstants.SECS_DIFFERENCE_BETWEEN_JULIAN_AND_UNIXTIME) * DateTimeConstants.USECS_PER_SEC);
  }

  /**
   * Calculate the time value(hour, minute, sec, fsec)
   * If tm.TomeZone is set, the result value is adjusted.
   * @param tm
   * @return
   */
  public static long toTime(TimeMeta tm) {
    if (tm.timeZone != 0 && tm.timeZone != Integer.MAX_VALUE) {
      int timeZoneSecs = tm.timeZone;
      tm.timeZone = Integer.MAX_VALUE;
      tm.plusMillis(0 - timeZoneSecs * 1000);
    }
    return toTime(tm.hours, tm.minutes, tm.secs, tm.fsecs);
  }

  /**
   * Calculate the time value(hour, minute, sec, fsec)
   * @param hour
   * @param min
   * @param sec
   * @param fsec
   * @return
   */
  public static long toTime(int hour, int min, int sec, int fsec) {
    return (((((hour * DateTimeConstants.MINS_PER_HOUR) + min) *
        DateTimeConstants.SECS_PER_MINUTE) + sec) *
        DateTimeConstants.USECS_PER_SEC) + fsec;
  }

  public static long toJavaTime(int hour, int min, int sec, int fsec) {
    return toTime(hour, min, sec, fsec)/DateTimeConstants.MSECS_PER_SEC;
  }

  /**
   * Calculate julian timestamp.
   * @param years
   * @param months
   * @param days
   * @param hours
   * @param minutes
   * @param seconds
   * @param fsec
   * @return
   */
  public static long toJulianTimestamp(
      int years, int months, int days, int hours, int minutes, int seconds, int fsec) {
    /* Julian day routines are not correct for negative Julian days_full */
    if (!isValidJulianDate(years, months, days)) {
      throw new ValueOutOfRangeException("Out of Range Julian days_full");
    }

    long numJulianDays = date2j(years, months, days) - DateTimeConstants.POSTGRES_EPOCH_JDATE;

    return toJulianTimestamp(numJulianDays, hours, minutes, seconds, fsec);
  }

  /**
   * Calculate julian timestamp.
   * @param numJulianDays
   * @param hours
   * @param minutes
   * @param seconds
   * @param fsec
   * @return
   */
  private static long toJulianTimestamp(long numJulianDays, int hours, int minutes, int seconds, int fsec) {
    long time = toTime(hours, minutes, seconds, fsec);

    long timestamp = numJulianDays * DateTimeConstants.USECS_PER_DAY + time;
      /* check for major overflow */
    if ((timestamp - time) / DateTimeConstants.USECS_PER_DAY != numJulianDays) {
      throw new RuntimeException("Out of Range of Time");
    }
      /* check for just-barely overflow (okay except time-of-day wraps) */
      /* caution: we want to allow 1999-12-31 24:00:00 */
    if ((timestamp < 0 && numJulianDays > 0) || (timestamp > 0 && numJulianDays < -1)) {
      throw new RuntimeException("Out of Range of Date");
    }

    return timestamp;
  }

  /**
   * Calculate julian timestamp.
   * If tm.TomeZone is set, the result value is adjusted.
   * @param tm
   * @return
   */
  public static long toJulianTimestamp(TimeMeta tm) {
    if (tm.timeZone != 0 && tm.timeZone != Integer.MAX_VALUE) {
      int timeZoneSecs = tm.timeZone;
      tm.timeZone = Integer.MAX_VALUE;
      tm.plusMillis(0 - timeZoneSecs * 1000);
    }
    if (tm.dayOfYear > 0) {
      return toJulianTimestamp(date2j(tm.years, 1, 1) + tm.dayOfYear - 1, tm.hours, tm.minutes, tm.secs, tm.fsecs);
    } else {
      return toJulianTimestamp(tm.years, tm.monthOfYear, tm.dayOfMonth, tm.hours, tm.minutes, tm.secs, tm.fsecs);
    }
  }

  /**
   * Set TimeMeta's field value using given julian timestamp.
   * Note that year is _not_ 1900-based, but is an explicit full value.
   * Also, month is one-based, _not_ zero-based.
   * Returns:
   *	 0 on success
   *	-1 on out of range
   *
   * If attimezone is NULL, the global timezone (including possibly brute forced
   * timezone) will be used.
   */
  public static void toJulianTimeMeta(long julianTimestamp, TimeMeta tm) {
    long date;
    long time;

    // TODO - If timezone is set, timestamp value should be adjusted here.
    time = julianTimestamp;

    // TMODULO
    date = time / DateTimeConstants.USECS_PER_DAY;
    if (date != 0) {
      time -= date * DateTimeConstants.USECS_PER_DAY;
    }
    if (time < 0) {
      time += DateTimeConstants.USECS_PER_DAY;
      date -= 1;
    }

    /* add offset to go from J2000 back to standard Julian date */
    date += DateTimeConstants.POSTGRES_EPOCH_JDATE;

    /* Julian day routine does not work for negative Julian days_full */
    if (date < 0 || date > Integer.MAX_VALUE) {
      throw new RuntimeException("Timestamp Out Of Scope");
    }

    j2date((int) date, tm);
    date2j(time, tm);
  }

  /**
   * This method is originated from dt2time in timestamp.c of PostgreSQL.
   *
   * @param julianDate
   * @return hour, min, sec, fsec
   */
  public static void date2j(long julianDate, TimeMeta tm) {
    long time = julianDate;

    tm.hours = (int) (time / DateTimeConstants.USECS_PER_HOUR);
    time -= tm.hours * DateTimeConstants.USECS_PER_HOUR;
    tm.minutes = (int) (time / DateTimeConstants.USECS_PER_MINUTE);
    time -= tm.minutes * DateTimeConstants.USECS_PER_MINUTE;
    tm.secs = (int) (time / DateTimeConstants.USECS_PER_SEC);
    tm.fsecs = (int) (time - (tm.secs * DateTimeConstants.USECS_PER_SEC));
  }

  /**
   * Decode date string which includes delimiters.
   *
   * This method is originated from DecodeDate() in datetime.c of PostgreSQL.
   * @param str The date string like '2013-12-25'.
   * @param fmask
   * @param tmaskValue
   * @param is2digits
   * @param tm
   */
  private static void decodeDate(String str, int fmask, AtomicInteger tmaskValue,  AtomicBoolean is2digits, TimeMeta tm) {

    int idx = 0;
    int nf = 0;
    TokenField type = null;
    int val = 0;

    AtomicInteger dmask = new AtomicInteger(0);
    int tmask = tmaskValue.get();
    boolean haveTextMonth = false;

    int length = str.length();
    char[] dateStr = str.toCharArray();
    String[] fields = new String[MAXDATEFIELDS];

    while(idx < length && nf < MAXDATEFIELDS) {

      /* skip field separators */
      while (idx < length && !Character.isLetterOrDigit(dateStr[idx])) {
        idx++;
      }

      if (idx == length) {
        throw new IllegalArgumentException("BAD Format: " + str);
      }

      int fieldStartIdx = idx;
      int fieldLength = idx;
      if (Character.isDigit(dateStr[idx])) {
        while (idx < length && Character.isDigit(dateStr[idx])) {
          idx++;
        }
        fieldLength = idx;
      } else if (Character.isLetterOrDigit(dateStr[idx])) {
        while (idx < length && Character.isLetterOrDigit(dateStr[idx])) {
          idx++;
        }
        fieldLength = idx;
      }

      fields[nf] = str.substring(fieldStartIdx, fieldLength);
      nf++;
    }

    /* look first for text fields, since that will be unambiguous month */
    for (int i = 0; i < nf; i++) {
      if (Character.isLetter(fields[i].charAt(0))) {
        DateToken dateToken =  DateTimeConstants.dateTokenMap.get(fields[i].toLowerCase());
        type = dateToken.getType();

        if (type == TokenField.IGNORE_DTF) {
          continue;
        }

        dmask.set(DateTimeConstants.DTK_M(type));
        switch (type) {
          case MONTH:
            tm.monthOfYear = type.getValue();
            haveTextMonth = true;
            break;

          default:
            throw new IllegalArgumentException("BAD Format: " + str);
        }
        if ((fmask & dmask.get()) != 0) {
          throw new IllegalArgumentException("BAD Format: " + str);
        }

        fmask |= dmask.get();
        tmask |= dmask.get();

			/* mark this field as being completed */
        fields[i] = null;
      }
    }

    /* now pick up remaining numeric fields */
    for (int i = 0; i < nf; i++) {
      if (fields[i] == null) {
        continue;
      }

      length = fields[i].length();
      if (length  <= 0) {
        throw new IllegalArgumentException("BAD Format: " + str);
      }

      decodeNumber(length, fields[i], haveTextMonth, fmask, dmask, tm, new AtomicLong(0), is2digits);

      if ( (fmask & dmask.get()) != 0 ) {
        throw new IllegalArgumentException("BAD Format: " + str);
      }
      fmask |= dmask.get();
      tmask |= dmask.get();
    }

    tmaskValue.set(tmask);

    if ((fmask & ~(DateTimeConstants.DTK_M(TokenField.DOY) | DateTimeConstants.DTK_M(TokenField.TZ))) != DateTimeConstants.DTK_DATE_M) {
      throw new IllegalArgumentException("BAD Format: " + str);
    }
  }

  /**
   * Decode time string which includes delimiters.
   * Return 0 if okay, a DTERR code if not.
   *
   * Only check the lower limit on hours, since this same code can be
   * used to represent time spans.
   * @param str
   * @param fmask
   * @param range
   * @param tmask
   * @param tm
   * @param fsec
   */
  private static void decodeTime(String str, int fmask, int range,
             AtomicInteger tmask, TimeMeta tm, AtomicLong fsec) {
    StringBuilder cp = new StringBuilder();

    tmask.set(DateTimeConstants.DTK_TIME_M);

    tm.hours = strtoi(str, 0, cp);
    if (cp.charAt(0) != ':') {
      throw new IllegalArgumentException("BAD Format: " + str);
    }

    tm.minutes = strtoi(cp.toString(), 1, cp);

    if (cp.length() == 0) {
      tm.secs = 0;
      fsec.set(0);
		  /* If it's a MINUTE TO SECOND interval, take 2 fields as being mm:ss */
      if (range == (DateTimeConstants.INTERVAL_MASK(TokenField.MINUTE) | DateTimeConstants.INTERVAL_MASK(TokenField.SECOND))) {
        tm.secs = tm.minutes;
        tm.minutes = tm.hours;
        tm.hours = 0;
      }
    } else if (cp.charAt(0) == '.') {
		  /* always assume mm:ss.sss is MINUTE TO SECOND */
      ParseFractionalSecond(cp, fsec);
      tm.secs = tm.minutes;
      tm.minutes = tm.hours;
      tm.hours = 0;
    }
    else if (cp.charAt(0) == ':') {
      tm.secs = strtoi(cp.toString(), 1, cp);
      if (cp.length() == 0){
        fsec.set(0);
      } else if (cp.charAt(0) == '.') {
        ParseFractionalSecond(cp, fsec);
      } else{
        throw new IllegalArgumentException("BAD Format: " + str);
      }
    } else {
      throw new IllegalArgumentException("BAD Format: " + str);
    }

	/* do a sanity check */
    if (tm.hours < 0 || tm.minutes < 0 || tm.minutes > DateTimeConstants.MINS_PER_HOUR - 1 ||
        tm.secs < 0 || tm.secs > DateTimeConstants.SECS_PER_MINUTE ||
            fsec.get() < 0 ||
            fsec.get() > DateTimeConstants.USECS_PER_SEC) {
      throw new IllegalArgumentException("BAD Format: FIELD_OVERFLOW: " + str);
    }
  }

  /**
   * Parse datetime string to julian time.
   * The result is the UTC time basis.
   * @param str
   * @return
   */
  public static long toJulianTimestamp(String str) {
    TimeMeta tm = decodeDateTime(str, MAXDATEFIELDS);
    return toJulianTimestamp(tm);
  }

  public static TimeMeta decodeDateTime(String str) {
    return decodeDateTime(str, MAXDATEFIELDS);
  }

  /**
   * Break string into tokens based on a date/time context.
   *
   * This method is originated form ParseDateTime() in datetime.c of PostgreSQL.
   *
   * @param str The input string
   * @param maxFields
   */
  public static TimeMeta decodeDateTime(String str, int maxFields) {
    int idx = 0;
    int nf = 0;
    int length = str.length();
    char [] timeStr = str.toCharArray();
    String [] fields = new String[maxFields];
    TokenField[] fieldTypes = new TokenField[maxFields];

    while (idx < length) {

      /* Ignore spaces between fields */
      if (Character.isSpaceChar(timeStr[idx])) {
        idx++;
        continue;
      }

      /* Record start of current field */
      if (nf >= maxFields) {
        throw new IllegalArgumentException("Too many fields");
      }

      int startIdx = idx;

      //January 8, 1999
      /* leading digit? then date or time */
      if (Character.isDigit(timeStr[idx])) {
        idx++;
        while (idx < length && Character.isDigit(timeStr[idx])) {
          idx++;
        }

        if (idx < length && timeStr[idx] == ':') {
          fieldTypes[nf] = TokenField.DTK_TIME;

          while (idx <length && (Character.isDigit(timeStr[idx]) || timeStr[idx] == ':' || timeStr[idx] == '.')) {
            idx++;
          }
        }

        /* date field? allow embedded text month */
        else if (idx < length && (timeStr[idx] == '-' || timeStr[idx] == '/' || timeStr[idx] == '.')) {

          /* save delimiting character to use later */
          char delim = timeStr[idx];
          idx++;

          /* second field is all digits? then no embedded text month */
          if (Character.isDigit(timeStr[idx])) {
            fieldTypes[nf] = delim == '.' ? TokenField.DTK_NUMBER : TokenField.DTK_DATE;

            while (idx < length && Character.isDigit(timeStr[idx])) {
              idx++;
            }

            /*
					   * insist that the delimiters match to get a three-field
					   * date.
					   */
            if (idx < length && timeStr[idx] == delim) {
              fieldTypes[nf] = TokenField.DTK_DATE;
              idx++;
              while (idx < length && (Character.isDigit(timeStr[idx]) || timeStr[idx] == delim)) {
                idx++;
              }
            }
          } else {
            fieldTypes[nf] = TokenField.DTK_DATE;
            while (idx < length && Character.isLetterOrDigit(timeStr[idx]) || timeStr[idx] == delim) {
              idx++;
            }
          }
        } else {
          /*
			     * otherwise, number only and will determine year, month, day, or
			     * concatenated fields later...
			    */
          fieldTypes[nf] = TokenField.DTK_NUMBER;
        }
      }

      /* Leading decimal point? Then fractional seconds... */
      else if (timeStr[idx] == '.') {
        idx++;
        while (idx < length && Character.isDigit(timeStr[idx])) {
          idx++;
          continue;
        }
        fieldTypes[nf] = TokenField.DTK_NUMBER;
      }

      // text? then date string, month, day of week, special, or timezone
      else if (Character.isLetter(timeStr[idx])) {
        boolean isDate;
        idx++;
        while (idx < length && Character.isLetter(timeStr[idx])) {
          idx++;
        }

        // Dates can have embedded '-', '/', or '.' separators.  It could
        // also be a timezone name containing embedded '/', '+', '-', '_',
        // or ':' (but '_' or ':' can't be the first punctuation). If the
        // next character is a digit or '+', we need to check whether what
        // we have so far is a recognized non-timezone keyword --- if so,
        // don't believe that this is the start of a timezone.

        isDate = false;
        if (idx < length && (timeStr[idx] == '-' || timeStr[idx] == '/' || timeStr[idx] == '.')) {
          isDate = true;
        } else if (idx < length && (timeStr[idx] == '+' || Character.isDigit(timeStr[idx]))) {
          // The original ParseDateTime handles this case. But, we currently omit this case.
          throw new IllegalArgumentException("Cannot parse this datetime field " + str.substring(startIdx, idx));
        }

        if (isDate) {
          fieldTypes[nf] = TokenField.DTK_DATE;

          do {
            idx++;
          } while (idx <length && (timeStr[idx] == '+' || timeStr[idx] == '-' || timeStr[idx] == '/' ||
              timeStr[idx] == '_' || timeStr[idx] == '.' || timeStr[idx] == ':' ||
              Character.isLetterOrDigit(timeStr[idx])));
        } else {
          fieldTypes[nf] = TokenField.DTK_STRING;
        }
      }

      // sign? then special or numeric timezone
      else if (timeStr[idx] == '+' || timeStr[idx] == '-') {
        idx++;

        // soak up leading whitespace
        while (idx < length && Character.isSpaceChar(timeStr[idx])) {
          idx++;
        }

        // numeric timezone?
        // note that "DTK_TZ" could also be a signed float or yyyy-mm */
        if (idx < length && Character.isDigit(timeStr[idx])) {
          fieldTypes[nf] = TokenField.DTK_TZ;
          idx++;

          while (idx < length && (Character.isDigit(timeStr[idx]) || timeStr[idx] == ':' || timeStr[idx] == '.' ||
              timeStr[idx] == '-')) {
            idx++;
          }
        }
        /* special? */
        else if (idx < length && Character.isLetter(timeStr[idx])) {
          fieldTypes[nf] = TokenField.DTK_SPECIAL;
          idx++;

          while (idx < length && Character.isLetter(timeStr[idx])) {
            idx++;
          }
        } else {
          throw new IllegalArgumentException("BAD Format: " + str.substring(startIdx, idx));
        }
      }
      /* ignore other punctuation but use as delimiter */
      else if (isPunctuation(timeStr[idx])) {
        idx++;
        continue;
      } else {  // otherwise, something is not right...
        throw new IllegalArgumentException("BAD datetime format: " + str.substring(startIdx, idx));
      }

      fields[nf] = str.substring(startIdx, idx);
      nf++;
    }
    return decodeDateTime(fields, fieldTypes, nf);
  }

  /**
   * Fetch a fractional-second value with suitable error checking
   * @param cp
   * @param fsec
   */
  public static void ParseFractionalSecond(StringBuilder cp, AtomicLong fsec) {
	  /* Caller should always pass the start of the fraction part */
    double frac = strtod(cp.toString(), 1, cp);
    fsec.set(Math.round(frac * 1000000));
  }

  /**
   * Interpret string as a numeric timezone.
   *
   * Return 0 if okay (and set *tzp), a DTERR code if not okay.
   *
   * NB: this must *not* ereport on failure; see commands/variable.c.
   * @param str
   * @param tz
   */
  public static void decodeTimezone(String str, AtomicInteger tz) {
    int min = 0;
    int sec = 0;
    StringBuilder sb = new StringBuilder();

    int strIndex = 0;
	  /* leading character must be "+" or "-" */
    if (str.charAt(strIndex) != '+' && str.charAt(strIndex) != '-') {
      throw new IllegalArgumentException("BAD Format: " + str);
    }
    int hr = strtoi(str, 1, sb);

	  /* explicit delimiter? */
    if (sb.length() > 0 && sb.charAt(0) == ':') {
      min = strtoi(sb.toString(), 1, sb);
      if (sb.charAt(0) == ':') {
        sec = strtoi(sb.toString(), 1, sb);
      }
    }
	  /* otherwise, might have run things together... */
    else if (sb.length() == 0 && str.length() > 3) {
      min = hr % 100;
      hr = hr / 100;
		/* we could, but don't, support a run-together hhmmss format */
    } else {
      min = 0;
    }
	  /* Range-check the values; see notes in datatype/timestamp.h */
    if (hr < 0 || hr > DateTimeConstants.MAX_TZDISP_HOUR) {
      throw new IllegalArgumentException("BAD Format: TZDISP_OVERFLOW: " + str);
    }
    if (min < 0 || min >= DateTimeConstants.MINS_PER_HOUR) {
      throw new IllegalArgumentException("BAD Format: TZDISP_OVERFLOW: " + str);
    }
    if (sec < 0 || sec >= DateTimeConstants.SECS_PER_MINUTE) {
      throw new IllegalArgumentException("BAD Format: TZDISP_OVERFLOW: " + str);
    }

    int tzValue = (hr * DateTimeConstants.MINS_PER_HOUR + min) * DateTimeConstants.SECS_PER_MINUTE + sec;
    if (str.charAt(strIndex) == '-') {
      tzValue = -tzValue;
    }
    tz.set(tzValue);
  }

  /**
   * Interpret plain numeric field as a date value in context.
   * @param flen
   * @param str
   * @param haveTextMonth
   * @param fmask
   * @param tmaskValue
   * @param tm
   * @param fsec
   * @param is2digits
   */
  private static void decodeNumber(int flen, String str, boolean haveTextMonth, int fmask,
               AtomicInteger tmaskValue, TimeMeta tm, AtomicLong fsec, AtomicBoolean is2digits) {
    int	val;
    StringBuilder cp = new StringBuilder();

    int tmask = 0;
    tmaskValue.set(tmask);

    val = strtoi(str, 0, cp);
    if (cp.toString().equals(str)) {
      throw new IllegalArgumentException("BAD Format: " + str);
    }

    if (cp.length() > 0 && cp.charAt(0) == '.') {
		/*
		 * More than two digits before decimal point? Then could be a date or
		 * a run-together time: 2001.360 20011225 040506.789
		 */
      if (cp.length() - str.length() > 2) {
        decodeNumberField(flen, str,
            (fmask | DateTimeConstants.DTK_DATE_M),
            tmaskValue, tm,
            fsec, is2digits);
        return;
      }
      ParseFractionalSecond(cp, fsec);
    }

  	// Special case for day of year
    if (flen == 3 && (fmask & DateTimeConstants.DTK_DATE_M) == DateTimeConstants.DTK_M(TokenField.YEAR) &&
        val >= 1 && val <= 366) {
      tmaskValue.set((DateTimeConstants.DTK_M(TokenField.DOY) |
          DateTimeConstants.DTK_M(TokenField.MONTH) |
          DateTimeConstants.DTK_M(TokenField.DAY)));
      tm.dayOfYear = val;
		  // tm_mon and tm_mday can't actually be set yet ...
      return;
    }

	  /* Switch based on what we have so far */
    int checkValue = fmask & DateTimeConstants.DTK_DATE_M;
    if (checkValue == 0) {
			/*
			 * Nothing so far; make a decision about what we think the input
			 * is.	There used to be lots of heuristics here, but the
			 * consensus now is to be paranoid.  It *must* be either
			 * YYYY-MM-DD (with a more-than-two-digit year field), or the
			 * field order defined by DateOrder.
			 */
      if (flen >= 3 || TajoConf.getDateOrder() == DateTimeConstants.DATEORDER_YMD) {
        tmaskValue.set(DateTimeConstants.DTK_M(TokenField.YEAR));
        tm.years = val;
      } else if (TajoConf.getDateOrder() == DateTimeConstants.DATEORDER_DMY) {
        tmaskValue.set(DateTimeConstants.DTK_M(TokenField.DAY));
        tm.dayOfMonth = val;
      } else {
        tmaskValue.set(DateTimeConstants.DTK_M(TokenField.MONTH));
        tm.monthOfYear = val;
      }
    } else if (checkValue == (DateTimeConstants.DTK_M(TokenField.YEAR))) {
			/* Must be at second field of YY-MM-DD */
      tmaskValue.set(DateTimeConstants.DTK_M(TokenField.MONTH));
      tm.monthOfYear = val;
    } else if (checkValue == (DateTimeConstants.DTK_M(TokenField.MONTH))) {
      if (haveTextMonth) {
				/*
				 * We are at the first numeric field of a date that included a
				 * textual month name.	We want to support the variants
				 * MON-DD-YYYY, DD-MON-YYYY, and YYYY-MON-DD as unambiguous
				 * inputs.	We will also accept MON-DD-YY or DD-MON-YY in
				 * either DMY or MDY modes, as well as YY-MON-DD in YMD mode.
				 */
        if (flen >= 3 || TajoConf.getDateOrder() == DateTimeConstants.DATEORDER_YMD) {
          tmaskValue.set(DateTimeConstants.DTK_M(TokenField.YEAR));
          tm.years = val;
        } else {
          tmaskValue.set(DateTimeConstants.DTK_M(TokenField.DAY));
          tm.dayOfMonth = val;
        }
      } else {
				/* Must be at second field of MM-DD-YY */
        tmaskValue.set(DateTimeConstants.DTK_M(TokenField.DAY));
        tm.dayOfMonth = val;
      }
    } else if (checkValue == (DateTimeConstants.DTK_M(TokenField.YEAR) | DateTimeConstants.DTK_M(TokenField.MONTH))) {
      if (haveTextMonth) {
				/* Need to accept DD-MON-YYYY even in YMD mode */
        if (flen >= 3 && is2digits.get()) {
					/* Guess that first numeric field is day was wrong */
          tmaskValue.set(DateTimeConstants.DTK_M(TokenField.DAY));		/* YEAR is already set */
          tm.dayOfMonth = tm.years;
          tm.years = val;
          is2digits.set(false);
        } else {
          tmaskValue.set(DateTimeConstants.DTK_M(TokenField.DAY));
          tm.dayOfMonth = val;
        }
      } else {
				/* Must be at third field of YY-MM-DD */
        tmaskValue.set(DateTimeConstants.DTK_M(TokenField.DAY));
        tm.dayOfMonth = val;
      }
    } else if (checkValue == DateTimeConstants.DTK_M(TokenField.DAY)) {
			/* Must be at second field of DD-MM-YY */
      tmaskValue.set(DateTimeConstants.DTK_M(TokenField.MONTH));
      tm.monthOfYear = val;
    } else if (checkValue == (DateTimeConstants.DTK_M(TokenField.MONTH) | DateTimeConstants.DTK_M(TokenField.DAY))) {
			/* Must be at third field of DD-MM-YY or MM-DD-YY */
      tmaskValue.set(DateTimeConstants.DTK_M(TokenField.YEAR));
      tm.years = val;
    } else if (checkValue == (DateTimeConstants.DTK_M(TokenField.YEAR) | DateTimeConstants.DTK_M(TokenField.MONTH) | DateTimeConstants.DTK_M(TokenField.DAY))) {
			/* we have all the date, so it must be a time field */
      decodeNumberField(flen, str, fmask,
          tmaskValue, tm,
          fsec, is2digits);
      return;

    } else {
      throw new IllegalArgumentException("BAD Format: " + str);
    }

	/*
	 * When processing a year field, mark it for adjustment if it's only one
	 * or two digits.
	 */
    if (tmaskValue.get() == DateTimeConstants.DTK_M(TokenField.YEAR)) {
      is2digits.set(flen <= 2);
    }
  }

  /**
   * Interpret numeric string as a concatenated date or time field.
   *
   * Use the context of previously decoded fields to help with
   * the interpretation.
   * @param len
   * @param str
   * @param fmask
   * @param tmaskValue
   * @param tm
   * @param fsec
   * @param is2digits
   * @return
   */
  static TokenField decodeNumberField(int len, String str, int fmask,
                    AtomicInteger tmaskValue, TimeMeta tm, AtomicLong fsec, AtomicBoolean is2digits) {
    /*
     * Have a decimal point? Then this is a date or something with a seconds
     * field...
     */
    int index = str.indexOf('.');

    if (index >= 0) {
      String cp = str.substring(index + 1);
		/*
		 * Can we use ParseFractionalSecond here?  Not clear whether trailing
		 * junk should be rejected ...
		 */
      double frac = strtod(cp, 0, null);
      fsec.set(Math.round(frac * 1000000));
		  /* Now truncate off the fraction for further processing */
      len = str.length();
    }
	  /* No decimal point and no complete date yet? */
    else if ((fmask & DateTimeConstants.DTK_DATE_M) != DateTimeConstants.DTK_DATE_M) {
		  /* yyyymmdd? */
      if (len == 8) {
        tmaskValue.set(DateTimeConstants.DTK_DATE_M);

        tm.dayOfMonth = Integer.parseInt(str.substring(6));
        tm.monthOfYear = Integer.parseInt(str.substring(4, 6));
        tm.years = Integer.parseInt(str.substring(0, 4));

        return TokenField.DTK_DATE;
      }
		  /* yymmdd? */
      else if (len == 6) {
        tmaskValue.set(DateTimeConstants.DTK_DATE_M);
        tm.dayOfMonth = Integer.parseInt(str.substring(4));
        tm.monthOfYear = Integer.parseInt(str.substring(2, 4));
        tm.years = Integer.parseInt(str.substring(0, 2));
        is2digits.set(true);

        return TokenField.DTK_DATE;
      }
    }

	  /* not all time fields are specified? */
    if ((fmask & DateTimeConstants.DTK_TIME_M) != DateTimeConstants.DTK_TIME_M) {
		  /* hhmmss */
      if (len == 6) {
        tmaskValue.set(DateTimeConstants.DTK_TIME_M);
        tm.secs = Integer.parseInt(str.substring(4));
        tm.minutes = Integer.parseInt(str.substring(2, 4));
        tm.hours = Integer.parseInt(str.substring(0, 2));

        return TokenField.DTK_TIME;
      }
		  /* hhmm? */
      else if (len == 4) {
        tmaskValue.set(DateTimeConstants.DTK_TIME_M);
        tm.secs = 0;
        tm.minutes = Integer.parseInt(str.substring(2, 4));
        tm.hours = Integer.parseInt(str.substring(0, 2));

        return TokenField.DTK_TIME;
      }
    }

    throw new IllegalArgumentException("BAD Format: " + str);
  }

  private static TimeMeta decodeDateTime(String[] fields, TokenField[] fieldTypes, int nf) {
    int	fmask = 0;
    AtomicInteger tmask = new AtomicInteger(0);
    int type;

    /* "prefix type" for ISO y2001m02d04 format */
    TokenField ptype = null;

    boolean		haveTextMonth = false;
    boolean		isjulian = false;
    AtomicBoolean is2digits = new AtomicBoolean(false);
    boolean		bc = false;

    int tzp = Integer.MAX_VALUE;
    String namedTimeZone = null;

    StringBuilder sb = new StringBuilder();
    // We'll insist on at least all of the date fields, but initialize the
    // remaining fields in case they are not set later...
    TokenField dtype = TokenField.DTK_DATE;
    TokenField mer = null;

    TimeMeta tm = new TimeMeta();
    TimeMeta cur_tm = new TimeMeta();

    AtomicLong fsec = new AtomicLong();
    AtomicInteger tz = new AtomicInteger(Integer.MAX_VALUE);

    // don't know daylight savings time status apriori */
    tm.isDST = false;

    for (int i = 0; i < nf; i++) {
      if (fieldTypes[i] == null) {
        continue;
      }
      switch (fieldTypes[i]) {
        case DTK_DATE:
          /***
           * Integral julian day with attached time zone?
           * All other forms with JD will be separated into
           * distinct fields, so we handle just this case here.
           ***/
          if (ptype == TokenField.DTK_JULIAN) {
            int			val;

            if (tzp == Integer.MAX_VALUE) {
              throw new IllegalArgumentException("BAD Format: " + fields[i]);
            }

            val = strtoi(fields[i], 0, sb);

            date2j(val, tm);
            isjulian = true;

					  /* Get the time zone from the end of the string */
            decodeTimezone(sb.toString(), tz);

            tmask.set(DateTimeConstants.DTK_DATE_M | DateTimeConstants.DTK_TIME_M | DateTimeConstants.DTK_M(TokenField.TZ));
            ptype = null;
            break;
          }
          /***
           * Already have a date? Then this might be a time zone name
           * with embedded punctuation (e.g. "America/New_York") or a
           * run-together time with trailing time zone (e.g. hhmmss-zz).
           * - thomas 2001-12-25
           *
           * We consider it a time zone if we already have month & day.
           * This is to allow the form "mmm dd hhmmss tz year", which
           * we've historically accepted.
           ***/
          else if (ptype != null ||
              ((fmask & (DateTimeConstants.DTK_M(TokenField.MONTH) | DateTimeConstants.DTK_M(TokenField.DAY))) ==
                  (DateTimeConstants.DTK_M(TokenField.MONTH) | DateTimeConstants.DTK_M(TokenField.DAY))))
          {
					/* No time zone accepted? Then quit... */
            if (tzp == Integer.MAX_VALUE) {
              throw new IllegalArgumentException("BAD Format: " + fields[i]);
            }

            if (Character.isDigit(fields[i].charAt(0)) || ptype != null) {
              if (ptype != null) {
							  /* Sanity check; should not fail this test */
                if (ptype != TokenField.DTK_TIME) {
                  throw new IllegalArgumentException("BAD Format: " + fields[i]);
                }
                ptype = null;
              }

						/*
						 * Starts with a digit but we already have a time
						 * field? Then we are in trouble with a date and time
						 * already...
						 */
              if ((fmask & DateTimeConstants.DTK_TIME_M) == DateTimeConstants.DTK_TIME_M) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }

              int index = fields[i].indexOf("-");
              if (index < 0) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }

						  /* Get the time zone from the end of the string */
              decodeTimezone(fields[i].substring(index + 1), tz);

              /*
               * Then read the rest of the field as a concatenated
               * time
               */
              decodeNumberField(fields[i].length(), fields[i],
                  fmask,
                  tmask, tm,
                  fsec, is2digits);

              /*
               * modify tmask after returning from
               * DecodeNumberField()
               */
              tmask.set(tmask.get() | DateTimeConstants.DTK_M(TokenField.TZ));
            }
            else {
              namedTimeZone = pg_tzset(fields[i]);
              if (namedTimeZone == null) {
							/*
							 * We should return an error code instead of
							 * ereport'ing directly, but then there is no way
							 * to report the bad time zone name.
							 */
                throw new IllegalArgumentException("BAD Format: time zone \"%s\" not recognized: " + fields[i]);
              }
						  /* we'll apply the zone setting below */
              tmask.set(DateTimeConstants.DTK_M(TokenField.TZ));
            }
          } else {
            decodeDate(fields[i], fmask, tmask, is2digits, tm);
          }
          break;

        case DTK_TIME:
          decodeTime(fields[i], (fmask | DateTimeConstants.DTK_DATE_M),
              DateTimeConstants.INTERVAL_FULL_RANGE,
              tmask, tm, fsec);
          break;

        case DTK_TZ: {
          decodeTimezone(fields[i], tz);
          tmask.set(DateTimeConstants.DTK_M(TokenField.TZ));
          break;
        }

        case DTK_NUMBER:

          /*
           * Was this an "ISO date" with embedded field labels? An
           * example is "y2001m02d04" - thomas 2001-02-04
           */
          if (ptype != null) {
            int val = strtoi(fields[i], 0, sb);

            /*
             * only a few kinds are allowed to have an embedded
             * decimal
             */
            if (sb.length() == 0) {
              continue;
            }
            if (sb.charAt(0) == '.') {
              switch (ptype) {
                case DTK_JULIAN:
                case DTK_TIME:
                case DTK_SECOND:
                  break;
                default:
                  throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }
            } else {
              throw new IllegalArgumentException("BAD Format: " + fields[i]);
            }
            switch (ptype) {
              case DTK_YEAR:
                tm.years = val;
                tmask.set(DateTimeConstants.DTK_M(TokenField.YEAR));
                break;

              case DTK_MONTH:

                /*
                 * already have a month and hour? then assume
                 * minutes
                 */
                if ((fmask & DateTimeConstants.DTK_M(TokenField.MONTH)) != 0 &&
                    (fmask & DateTimeConstants.DTK_M(TokenField.HOUR)) != 0) {
                  tm.minutes = val;
                  tmask.set(DateTimeConstants.DTK_M(TokenField.MINUTE));
                }
                else {
                  tm.monthOfYear = val;
                  tmask.set(DateTimeConstants.DTK_M(TokenField.MONTH));
                }
                break;

              case DTK_DAY:
                tm.dayOfMonth = val;
                tmask.set(DateTimeConstants.DTK_M(TokenField.DAY));
                break;

              case DTK_HOUR:
                tm.hours = val;
                tmask.set(DateTimeConstants.DTK_M(TokenField.HOUR));
                break;

              case DTK_MINUTE:
                tm.minutes = val;
                tmask.set(DateTimeConstants.DTK_M(TokenField.MINUTE));
                break;

              case DTK_SECOND:
                tm.secs = val;
                tmask.set(DateTimeConstants.DTK_M(TokenField.SECOND));
                if (sb.charAt(0) == '.') {
                  ParseFractionalSecond(sb, fsec);
                  tmask.set(DateTimeConstants.DTK_ALL_SECS_M);
                }
                break;
              case DTK_TZ:
                tmask.set(DateTimeConstants.DTK_M(TokenField.TZ));
                decodeTimezone(fields[i], tz);
                break;

              case DTK_JULIAN:
							  /* previous field was a label for "julian date" */
                if (val < 0) {
                  throw new IllegalArgumentException("BAD Format: FIELD_OVERFLOW: " + fields[i]);
                }
                tmask.set(DateTimeConstants.DTK_DATE_M);
                date2j(val, tm);
                isjulian = true;

							  /* fractional Julian Day? */
                if (sb.charAt(0) == '.') {
                  double time = strtod(sb.toString(), 0, sb);

                  time *= DateTimeConstants.USECS_PER_DAY;
                  date2j((long)time, tm);
                  tmask.set(tmask.get() | DateTimeConstants.DTK_TIME_M);
                }
                break;

              case DTK_TIME:
							/* previous field was "t" for ISO time */
                decodeNumberField(fields[i].length(), fields[i],
                    (fmask | DateTimeConstants.DTK_DATE_M),
                    tmask, tm,
                    fsec, is2digits);
                if (tmask.get() != DateTimeConstants.DTK_TIME_M) {
                  throw new IllegalArgumentException("BAD Format: FIELD_OVERFLOW: " + fields[i]);
                }
                break;

              default:
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
            }

            ptype = null;
            dtype = TokenField.DTK_DATE;
          } else {
            int flen = fields[i].length();
            int index = fields[i].indexOf(".");
            String cp = null;
            if (index > 0) {
              cp = fields[i].substring(index + 1);
            }

					  /* Embedded decimal and no date yet? */
            if (cp != null && ((fmask & DateTimeConstants.DTK_DATE_M) == 0 )) {
              decodeDate(fields[i], fmask,
                  tmask, is2digits, tm);
            }
					  /* embedded decimal and several digits before? */
            else if (cp != null && flen - cp.length() > 2) {
						/*
						 * Interpret as a concatenated date or time Set the
						 * type field to allow decoding other fields later.
						 * Example: 20011223 or 040506
						 */
              decodeNumberField(flen, fields[i], fmask,
                  tmask, tm,
                  fsec, is2digits);
            }
            else if (flen > 4) {
              decodeNumberField(flen, fields[i], fmask,
                  tmask, tm,
                  fsec, is2digits);
            }
					  /* otherwise it is a single date/time field... */
            else {
              decodeNumber(flen, fields[i],
                  haveTextMonth, fmask,
                  tmask, tm,
                  fsec, is2digits);
            }
          }
          break;
        case DTK_STRING:
        case DTK_SPECIAL:
          DateToken dateToken =  DateTimeConstants.dateTokenMap.get(fields[i].toLowerCase());
          if (dateToken == null) {
            throw new IllegalArgumentException("BAD Format: " + fields[i]);
          }
          tmask.set(DateTimeConstants.DTK_M(dateToken.getType()));
          switch (dateToken.getType()) {
            case RESERV:
              switch(dateToken.getValueType()) {
                case DTK_CURRENT:
                  throw new IllegalArgumentException("BAD Format: date/time value \"current\" is no longer supported" + fields[i]);

                case DTK_NOW:
                  tmask.set(DateTimeConstants.DTK_DATE_M | DateTimeConstants.DTK_TIME_M | DateTimeConstants.DTK_M(TokenField.TZ));
                  dtype = TokenField.DTK_DATE;
                  date2j(javaTimeToJulianTime(System.currentTimeMillis()), tm);
                  break;

                case DTK_YESTERDAY:
                  tmask.set(DateTimeConstants.DTK_DATE_M);
                  dtype = TokenField.DTK_DATE;
                  date2j(javaTimeToJulianTime(System.currentTimeMillis()), tm);
                  tm.plusDays(-1);
                  break;

                case DTK_TODAY:
                  tmask.set(DateTimeConstants.DTK_DATE_M);
                  dtype = TokenField.DTK_DATE;
                  date2j(javaTimeToJulianTime(System.currentTimeMillis()), cur_tm);
                  tm.years = cur_tm.years;
                  tm.monthOfYear = cur_tm.monthOfYear;
                  tm.dayOfMonth = cur_tm.dayOfMonth;
                  break;

                case DTK_TOMORROW:
                  tmask.set(DateTimeConstants.DTK_DATE_M);
                  dtype = TokenField.DTK_DATE;
                  date2j(javaTimeToJulianTime(System.currentTimeMillis()), tm);
                  tm.plusDays(1);
                  break;

                case DTK_ZULU:
                  tmask.set(DateTimeConstants.DTK_TIME_M | DateTimeConstants.DTK_M(TokenField.TZ));
                  dtype = TokenField.DTK_DATE;
                  tm.hours = 0;
                  tm.minutes = 0;
                  tm.secs = 0;
                  break;

                default:
                  dtype = dateToken.getValueType();
              }
              break;

            case MONTH:
              /*
               * already have a (numeric) month? then see if we can
               * substitute...
               */
              if ((fmask & DateTimeConstants.DTK_M(TokenField.MONTH)) != 0 && !haveTextMonth &&
                  (fmask & DateTimeConstants.DTK_M(TokenField.DAY)) == 0 &&
                  tm.monthOfYear >= 1 && tm.monthOfYear <= 31) {
                tm.dayOfMonth = tm.monthOfYear;
                tmask.set(DateTimeConstants.DTK_M(TokenField.DAY));
              }
              haveTextMonth = true;
              tm.monthOfYear = dateToken.getValue();
              break;

            case DTZMOD:

						/*
						 * daylight savings time modifier (solves "MET DST"
						 * syntax)
						 */
              tmask.set(tmask.get() | DateTimeConstants.DTK_M(TokenField.DTZ));
              tm.isDST = true;
              if (tzp == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }
              tzp += dateToken.getValue() * DateTimeConstants.MINS_PER_HOUR;
              break;

            case DTZ:

						/*
						 * set mask for TZ here _or_ check for DTZ later when
						 * getting default timezone
						 */
              tmask.set(tmask.get() | DateTimeConstants.DTK_M(TokenField.TZ));
              tm.isDST = true;
              if (tzp == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }
              tzp = dateToken.getValue() * DateTimeConstants.MINS_PER_HOUR;
              break;

            case TZ:
              tm.isDST = false;
              if (tzp == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }
              tzp = dateToken.getValue() * DateTimeConstants.MINS_PER_HOUR;
              break;

            case IGNORE_DTF:
              break;

            case AMPM:
              mer = dateToken.getValueType();
              break;

            case ADBC:
              bc = (dateToken.getValueType() == TokenField.BC);
              break;

            case DOW:
              tm.dayOfWeek = dateToken.getValue();
              break;

            case UNITS:
              tmask.set(0);
              ptype = dateToken.getValueType();
              break;

            case ISOTIME:

						/*
						 * This is a filler field "t" indicating that the next
						 * field is time. Try to verify that this is sensible.
						 */
              tmask.set(0);

						/* No preceding date? Then quit... */
              if ((fmask & DateTimeConstants.DTK_DATE_M) != DateTimeConstants.DTK_DATE_M) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }

              /***
               * We will need one of the following fields:
               *	DTK_NUMBER should be hhmmss.fff
               *	DTK_TIME should be hh:mm:ss.fff
               *	DTK_DATE should be hhmmss-zz
               ***/
              if (i >= nf - 1 ||
                  (fieldTypes[i + 1] != TokenField.DTK_NUMBER &&
                      fieldTypes[i + 1] != TokenField.DTK_TIME &&
                      fieldTypes[i + 1] != TokenField.DTK_DATE)) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }

              ptype = dateToken.getValueType();
              break;

            case UNKNOWN_FIELD:

						/*
						 * Before giving up and declaring error, check to see
						 * if it is an all-alpha timezone name.
						 */
              namedTimeZone = pg_tzset(fields[i]);
              if (namedTimeZone == null) {
                throw new IllegalArgumentException("BAD Format: " + fields[i]);
              }
						/* we'll apply the zone setting below */
              tmask.set(DateTimeConstants.DTK_M(TokenField.TZ));
              break;

            default:
              throw new IllegalArgumentException("BAD Format: " + fields[i]);
          }
          break;
      }
      if ((tmask.get() & fmask) != 0) {
        throw new IllegalArgumentException("BAD Format: " + fields[i]);
      }
      fmask |= tmask.get();
    }   /* end loop over fields */

    tm.fsecs = fsec.intValue();
    tm.timeZone = tz.get();
    /* do final checking/adjustment of Y/M/D fields */
    validateDate(fmask, isjulian, is2digits.get(), bc, tm);

	  /* handle AM/PM */
    if (mer != null && mer != TokenField.HR24 && tm.hours > DateTimeConstants.HOURS_PER_DAY / 2) {
      throw new IllegalArgumentException("BAD Format: overflow hour: " + tm.hours);
    }
    if (mer != null && mer == TokenField.AM && tm.hours == DateTimeConstants.HOURS_PER_DAY / 2) {
      tm.hours = 0;
    } else if (mer != null && mer == TokenField.PM && tm.hours != DateTimeConstants.HOURS_PER_DAY / 2) {
      tm.hours += DateTimeConstants.HOURS_PER_DAY / 2;
    }
	  /* do additional checking for full date specs... */
    if (dtype == TokenField.DTK_DATE) {
      if ((fmask & DateTimeConstants.DTK_DATE_M) != DateTimeConstants.DTK_DATE_M) {
        if ((fmask & DateTimeConstants.DTK_TIME_M) == DateTimeConstants.DTK_TIME_M) {
          return tm;
        }
        throw new IllegalArgumentException("BAD Format: " + tm);
      }

		/*
		 * If we had a full timezone spec, compute the offset (we could not do
		 * it before, because we need the date to resolve DST status).
		 */
      if (namedTimeZone != null) {
			/* daylight savings time modifier disallowed with full TZ */
        if ( (fmask & DateTimeConstants.DTK_M(TokenField.DTZMOD)) != 0 ) {
          throw new IllegalArgumentException("BAD Format: " + tm);
        }
      }
    }

    return tm;
  }

  private static String pg_tzset(String str) {
    //TODO implements logic
    return null;
  }

  /**
   * Check valid year/month/day values, handle BC and DOY cases
   * Return 0 if okay, a DTERR code if not.
   * @param fmask
   * @param isjulian
   * @param is2digits
   * @param bc
   * @param tm
   * @return
   */
  private static int validateDate(int fmask, boolean isjulian, boolean is2digits, boolean bc, TimeMeta tm) {
    if ( (fmask & DateTimeConstants.DTK_M(TokenField.YEAR)) != 0 ) {
      if (isjulian) {
			/* tm_year is correct and should not be touched */
      } else if (bc) {
			  /* there is no year zero in AD/BC notation */
        if (tm.years <= 0) {
          throw new IllegalArgumentException("BAD Format: year overflow:" + tm.years);
        }
			  /* internally, we represent 1 BC as year zero, 2 BC as -1, etc */
        tm.years = -(tm.years - 1);
      }
      else if (is2digits) {
			  /* process 1 or 2-digit input as 1970-2069 AD, allow '0' and '00' */
        if (tm.years < 0) { /* just paranoia */
          throw new IllegalArgumentException("BAD Format: year overflow:" + tm.years);
        }
        if (tm.years < 70) {
          tm.years += 2000;
        } else if (tm.years < 100) {
          tm.years += 1900;
        }
      }
      else {
			  /* there is no year zero in AD/BC notation */
        if (tm.years <= 0) {
          throw new IllegalArgumentException("BAD Format: year overflow:" + tm.years);
        }
      }
    }

	  /* now that we have correct year, decode DOY */
    if ( (fmask & DateTimeConstants.DTK_M(TokenField.DOY)) != 0 ) {
      j2date(date2j(tm.years, 1, 1) + tm.dayOfYear - 1, tm);
    }

	  /* check for valid month */
    if ( (fmask & DateTimeConstants.DTK_M(TokenField.MONTH)) != 0 ) {
      if (tm.monthOfYear < 1 || tm.monthOfYear > DateTimeConstants.MONTHS_PER_YEAR) {
        throw new IllegalArgumentException("BAD Format: month overflow:" + tm.monthOfYear);
      }
    }

	  /* minimal check for valid day */
    if ( (fmask & DateTimeConstants.DTK_M(TokenField.DAY)) != 0 ) {
      if (tm.dayOfMonth < 1 || tm.dayOfMonth > 31) {
        throw new IllegalArgumentException("BAD Format: day overflow:" + tm.dayOfMonth);
      }
    }

    if ((fmask & DateTimeConstants.DTK_DATE_M) == DateTimeConstants.DTK_DATE_M) {
		/*
		 * Check for valid day of month, now that we know for sure the month
		 * and year.  Note we don't use MD_FIELD_OVERFLOW here, since it seems
		 * unlikely that "Feb 29" is a YMD-order error.
		 */
      boolean leapYear = isLeapYear(tm.years);
      if (tm.dayOfMonth > DateTimeConstants.DAY_OF_MONTH[leapYear ? 1: 0][tm.monthOfYear - 1])
        throw new IllegalArgumentException("BAD Format: day overflow:" + tm.dayOfMonth);
    }

    return 0;
  }

  public static int strtoi(String str, int startIndex, StringBuilder sb) {
    sb.setLength(0);
    char[] chars = str.toCharArray();

    int index = startIndex;
    for (; index < chars.length; index++) {
      if (!Character.isDigit(chars[index])) {
        break;
      }
    }

    int val = index == startIndex ? 0 : Integer.parseInt(str.substring(startIndex, index));
    sb.append(chars, index, chars.length - index);

    return val;
  }

  public static long strtol(String str, int startIndex, StringBuilder sb) {
    sb.setLength(0);
    char[] chars = str.toCharArray();

    int index = startIndex;
    for (; index < chars.length; index++) {
      if (!Character.isDigit(chars[index])) {
        break;
      }
    }

    long val = index == startIndex ? 0 : Long.parseLong(str.substring(startIndex, index));
    sb.append(chars, index, chars.length - index);

    return val;
  }

  public static double strtod(String str, int strIndex, StringBuilder sb) {
    if (sb != null) {
      sb.setLength(0);
    }
    char[] chars = str.toCharArray();

    int index = strIndex;
    for (; index < chars.length; index++) {
      if (!Character.isDigit(chars[index])) {
        break;
      }
    }

    double val = Double.parseDouble(str.substring(0, index));
    if (sb != null) {
      sb.append(chars, index, chars.length - index);
    }
    return val;
  }

  /**
   * Check whether it is a punctuation character or not.
   * @param c The character to be checked
   * @return True if it is a punctuation character. Otherwise, false.
   */
  public static boolean isPunctuation(char c) {
    return ((c >= '!' && c <= '/') ||
        (c >= ':' && c <= '@') ||
        (c >= '[' && c <= '`') ||
        (c >= '{' && c <= '~'));
  }

  public static String toString(TimeMeta tm) {
    return encodeDateTime(tm, DateStyle.ISO_DATES);
  }

  /**
   * Encode date and time interpreted as local time.
   *
   * tm and fsec are the value to encode, print_tz determines whether to include
   * a time zone (the difference between timestamp and timestamptz types), tz is
   * the numeric time zone offset, tzn is the textual time zone, which if
   * specified will be used instead of tz by some styles, style is the date
   * style, str is where to write the output.
   *
   * Supported date styles:
   *	Postgres - day mon hh:mm:ss yyyy tz
   *	SQL - mm/dd/yyyy hh:mm:ss.ss tz
   *	ISO - yyyy-mm-dd hh:mm:ss+/-tz
   *	German - dd.mm.yyyy hh:mm:ss tz
   *	XSD - yyyy-mm-ddThh:mm:ss.ss+/-tz
   *
   * This method is originated from EncodeDateTime of datetime.c of PostgreSQL.
   * @param tm
   * @param style
   * @return
   */
  public static String encodeDateTime(TimeMeta tm, DateStyle style) {

    StringBuilder sb = new StringBuilder();
    switch (style) {

      case ISO_DATES:
      case XSO_DATES:
        if (style == DateTimeConstants.DateStyle.ISO_DATES) {
          sb.append(String.format("%04d-%02d-%02d %02d:%02d:",
              (tm.years > 0) ? tm.years : -(tm.years - 1),
              tm.monthOfYear, tm.dayOfMonth, tm.hours, tm.minutes));
        } else {
          sb.append(String.format("%04d-%02d-%02dT%02d:%02d:",
              (tm.years > 0) ? tm.years : -(tm.years - 1),
              tm.monthOfYear, tm.dayOfMonth, tm.hours, tm.minutes));
        }

        appendSecondsToEncodeOutput(sb, tm.secs, tm.fsecs, 6, true);
        if (tm.timeZone != 0 && tm.timeZone != Integer.MAX_VALUE) {
          sb.append(getTimeZoneDisplayTime(tm.timeZone));
        }
        if (tm.years <= 0) {
          sb.append(" BC");
        }
        break;

      case SQL_DATES:
        // Compatible with Oracle/Ingres date formats

    }

    return sb.toString();
  }

  public static String encodeDate(TimeMeta tm, DateStyle style) {
    StringBuilder sb = new StringBuilder();
    switch (style) {
      case ISO_DATES:
      case XSO_DATES:
      case SQL_DATES:
        // Compatible with Oracle/Ingres date formats
      default:
        sb.append(String.format("%04d-%02d-%02d",
            (tm.years > 0) ? tm.years : -(tm.years - 1),
            tm.monthOfYear, tm.dayOfMonth));
    }

    return sb.toString();
  }

  public static String encodeTime(TimeMeta tm, DateStyle style) {
    StringBuilder sb = new StringBuilder();
    switch (style) {

      case ISO_DATES:
      case XSO_DATES:
      case SQL_DATES:
        // Compatible with Oracle/Ingres date formats
      default :
        sb.append(String.format("%02d:%02d:", tm.hours, tm.minutes));
        appendSecondsToEncodeOutput(sb, tm.secs, tm.fsecs, 6, true);
        if (tm.timeZone != 0 && tm.timeZone != Integer.MAX_VALUE) {
          sb.append(getTimeZoneDisplayTime(tm.timeZone));
        }
        break;
    }

    return sb.toString();
  }

  /**
   * Append sections and fractional seconds (if any) at *cp.
   * precision is the max number of fraction digits, fillzeros says to
   * pad to two integral-seconds digits.
   * Note that any sign is stripped from the input seconds values.
   *
   * This method is originated form AppendSeconds in datetime.c of PostgreSQL.
   */
  public static void appendSecondsToEncodeOutput(
      StringBuilder sb, int sec, int fsec, int precision, boolean fillzeros) {
    if (fsec == 0) {
      if (fillzeros)
        sb.append(String.format("%02d", Math.abs(sec)));
      else
        sb.append(String.format("%d", Math.abs(sec)));
    } else {
      if (fillzeros) {
        sb.append(String.format("%02d", Math.abs(sec)));
      } else {
        sb.append(String.format("%d", Math.abs(sec)));
      }

      if (precision > MAX_FRACTION_LENGTH) {
        precision = MAX_FRACTION_LENGTH;
      }

      if (precision > 0) {
        char[] fracChars = String.valueOf(fsec).toCharArray();
        char[] resultChars = new char[MAX_FRACTION_LENGTH];

        int numFillZero = MAX_FRACTION_LENGTH - fracChars.length;
        for (int i = 0, fracIdx = 0; i < MAX_FRACTION_LENGTH; i++) {
          if (i < numFillZero) {
            resultChars[i] = '0';
          } else {
            resultChars[i] = fracChars[fracIdx];
            fracIdx++;
          }
        }
        sb.append(".").append(resultChars, 0, precision);
      }
      trimTrailingZeros(sb);
    }
  }

  /**
   * ... resulting from printing numbers with full precision.
   *
   * Before Postgres 8.4, this always left at least 2 fractional digits,
   * but conversations on the lists suggest this isn't desired
   * since showing '0.10' is misleading with values of precision(1).
   *
   * This method is originated form AppendSeconds in datetime.c of PostgreSQL.
   * @param sb
   */
  public static void trimTrailingZeros(StringBuilder sb) {
    int len = sb.length();
    while (len > 1 && sb.charAt(len - 1) == '0' && sb.charAt(len - 2) != '.') {
      len--;
      sb.setLength(len);
    }
  }

  /**
   * Return the Julian day which corresponds to the first day (Monday) of the given ISO 8601 year and week.
   * Julian days_full are used to convert between ISO week dates and Gregorian dates.
   *
   * This method is originated form AppendSeconds in timestamp.c of PostgreSQL.
   * @param year
   * @param week
   * @return
   */
  public static int isoweek2j(int year, int week) {
	  /* fourth day of current year */
    int day4 = date2j(year, 1, 4);

	  /* day0 == offset to first day of week (Monday) */
    int day0 = j2day(day4 - 1);

    return ((week - 1) * 7) + (day4 - day0);
  }

  /**
   * Convert ISO week of year number to date.
   * The year field must be specified with the ISO year!
   * karel 2000/08/07
   *
   * This method is originated form AppendSeconds in timestamp.c of PostgreSQL.
   * @param woy
   * @param tm
   */
  public static void isoweek2date(int woy, TimeMeta tm) {
    j2date(isoweek2j(tm.years, woy), tm);
  }

  /**
   * Convert an ISO 8601 week date (ISO year, ISO week) into a Gregorian date.
   * Gregorian day of week sent so weekday strings can be supplied.
   * Populates year, mon, and mday with the correct Gregorian values.
   * year must be passed in as the ISO year.
   *
   * This method is originated form AppendSeconds in timestamp.c of PostgreSQL.
   * @param isoweek
   * @param wday
   * @param tm
   */
  public static void isoweekdate2date(int isoweek, int wday, TimeMeta tm) {
    int jday;
    jday = isoweek2j(tm.years, isoweek);
	  /* convert Gregorian week start (Sunday=1) to ISO week start (Monday=1) */
    if (wday > 1) {
      jday += wday - 2;
    } else {
      jday += 6;
    }
    j2date(jday, tm);
  }

  /**
   * Returns the ISO 8601 day-of-year, given a Gregorian year, month and day.
   * Possible return values are 1 through 371 (364 in non-leap years).
   * @param year
   * @param mon
   * @param mday
   * @return
   */
  public static int date2isoyearday(int year, int mon, int mday) {
    return date2j(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1;
  }

  public static void toUserTimezone(TimeMeta tm) {
    toUserTimezone(tm, TajoConf.getCurrentTimeZone());
  }

  public static void toUserTimezone(TimeMeta tm, TimeZone timeZone) {
    tm.plusMillis(timeZone.getRawOffset());
  }

  public static void toUTCTimezone(TimeMeta tm) {
    TimeZone timeZone = TajoConf.getCurrentTimeZone();
    tm.plusMillis(0 - timeZone.getRawOffset());
  }

  public static String getTimeZoneDisplayTime(TimeZone timeZone) {
    return getTimeZoneDisplayTime(timeZone.getRawOffset() / 1000);
  }

  public static String getTimeZoneDisplayTime(int totalSecs) {
    if (totalSecs == 0) {
      return "";
    }
    int minutes = Math.abs(totalSecs) / DateTimeConstants.SECS_PER_MINUTE;
    int hours = minutes / DateTimeConstants.MINS_PER_HOUR;
    minutes = minutes - hours * DateTimeConstants.MINS_PER_HOUR;

    StringBuilder sb = new StringBuilder();
    String prefix = "";

    sb.append(totalSecs > 0 ? "+" : "-").append(String.format("%02d", hours));

    if (minutes > 0) {
      sb.append(":").append(String.format("%02d", minutes));
      prefix = ":";
    }

    return sb.toString();
  }

  public static long getDay(DateTime dateTime) {
    return convertToMicroSeconds(dateTime.withTimeAtStartOfDay());
  }

  public static long getHour(DateTime dateTime) {
    return convertToMicroSeconds(dateTime.withTime(dateTime.get(org.joda.time.DateTimeFieldType.hourOfDay()), 0, 0, 0));
  }

  public static long getMinute(DateTime dateTime) {
    return convertToMicroSeconds(dateTime.withTime(dateTime.get(org.joda.time.DateTimeFieldType.hourOfDay()),
        dateTime.get(org.joda.time.DateTimeFieldType.minuteOfHour()), 0, 0));
  }

  public static long getSecond(DateTime dateTime) {
    return convertToMicroSeconds(dateTime.withTime(dateTime.get(org.joda.time.DateTimeFieldType.hourOfDay()),
        dateTime.get(org.joda.time.DateTimeFieldType.minuteOfHour()), dateTime.get(org.joda.time.DateTimeFieldType.secondOfMinute()), 0));
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
