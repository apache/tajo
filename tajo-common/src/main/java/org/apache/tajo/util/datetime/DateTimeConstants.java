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

import java.util.HashMap;
import java.util.Map;

public class DateTimeConstants {
  public enum DateStyle {
    XSO_DATES,
    ISO_DATES,
    SQL_DATES
  };

  public static final int[][]	DAY_OF_MONTH = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}
  };

  /** assumes leap year every four years */
  public static final double DAYS_PER_YEAR = 365.25;
  public static final int MONTHS_PER_YEAR = 12;


  // DAYS_PER_MONTH is very imprecise.  The more accurate value is
  // 365.2425/12 = 30.436875, or '30 days_full 10:29:06'.  Right now we only
  // return an integral number of days_full, but someday perhaps we should
  // also return a 'time' value to be used as well.	ISO 8601 suggests
  // 0 days_full.

  /** assumes exactly 30 days_full per month */
  public static final int DAYS_PER_MONTH	= 30;
  /** assume no daylight savings time changes */
  public static final int HOURS_PER_DAY = 24;

  // This doesn't adjust for uneven daylight savings time intervals or leap
  // seconds, and it crudely estimates leap years.  A more accurate value
  // for days_full per years is 365.2422.

  /** avoid floating-point computation */
  public static final int SECS_PER_YEAR	= 36525 * 864;
  public static final int SECS_PER_DAY = 86_400;
  public static final int SECS_PER_HOUR	= 3600;
  public static final int SECS_PER_MINUTE = 60;
  public static final int MINS_PER_HOUR	= 60;

  public static final long MSECS_PER_DAY = 86_400_000L;
  public static final long MSECS_PER_SEC = 1000L;

  public static final long USECS_PER_DAY = 86_400_000_000L;
  public static final long USECS_PER_HOUR	= 3_600_000_000L;
  public static final long USECS_PER_MINUTE = 60_000_000L;
  public static final long USECS_PER_SEC = 1_000_000L;
  public static final long USECS_PER_MSEC = 1000L;

  public static final int JULIAN_MINYEAR = -4713;
  public static final int JULIAN_MINMONTH = 11;
  public static final int JULIAN_MINDAY = 24;
  public static final int JULIAN_MAXYEAR = 5874898;

  /** == DateTimeUtil.toJulianDate(JULIAN_MAXYEAR, 1, 1) */
  public static final int JULIAN_MAX = 2147483494;

  // This is an implementation copied from PGStatement in pgsql jdbc
  // We can't use Long.MAX_VALUE or Long.MIN_VALUE for java.sql.date
  // because this would break the 'normalization contract' of the
  // java.sql.Date API.
  // The follow values are the nearest MAX/MIN values with hour,
  // minute, second, millisecond set to 0 - this is used for
  // -infinity / infinity representation in Java
  public static final long DATE_POSITIVE_INFINITY = 9223372036825200000l;
  public static final long DATE_NEGATIVE_INFINITY = -9223372036832400000l;
  
  /** the first ISO day of week */
  public static final int MONDAY = 1;
  
  /** the second ISO day of week */
  public static final int TUESDAY = 2;

  /** the third ISO day of week */
  public static final int WEDNESDAY = 3;

  /** the fourth ISO day of week */
  public static final int THURSDAY = 4;

  /** the fifth ISO day of week */
  public static final int FRIDAY = 5;

  /** the sixth ISO day of week */
  public static final int SATURDAY = 6;

  /** the seventh ISO day of week */
  public static final int SUNDAY = 7;

  // Julian-date equivalents of Day 0 in Unix and Postgres reckoning
  /** == DateTimeUtil.toJulianDate(1970, 1, 1) */
  public static final int UNIX_EPOCH_JDATE =     2_440_588;
  /** == DateTimeUtil.toJulianDate(2000, 1, 1) */
  public static final int POSTGRES_EPOCH_JDATE = 2_451_545;
  /** == (POSTGRES_EPOCH_JDATE * SECS_PER_DAY) - (UNIX_EPOCH_JDATE * SECS_PER_DAY); */
  public static final long SECS_DIFFERENCE_BETWEEN_JULIAN_AND_UNIXTIME = 946_684_800;

  public static final int MAX_TZDISP_HOUR	=	15;	/* maximum allowed hour part */
  public static final int TZDISP_LIMIT =	((MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR);

  public static final int INTERVAL_FULL_RANGE = 0x7FFF;

  public static final String DAGO =			"ago";
  public static final String DCURRENT =		"current";
  public static final String EPOCH =			"epoch";
  public static final String INVALID =			"invalid";
  public static final String EARLY =			"-infinity";
  public static final String LATE	=		"infinity";
  public static final String NOW =				"now";
  public static final String TODAY	=		"today";
  public static final String TOMORROW	=	"tomorrow";
  public static final String YESTERDAY =		"yesterday";
  public static final String ZULU	 =		"zulu";

  public static final String DMICROSEC	=	"usecond";
  public static final String DMILLISEC =		"msecond";
  public static final String DSECOND	=		"second";
  public static final String DMINUTE =			"minute";
  public static final String DHOUR	=		"hour";
  public static final String DDAY	 =		"day";
  public static final String DWEEK	=		"week";
  public static final String DMONTH	=		"month";
  public static final String DQUARTER	=	"quarter";
  public static final String DYEAR		=	"year";
  public static final String DDECADE	=		"decade";
  public static final String DCENTURY	=	"century";
  public static final String DMILLENNIUM	=	"millennium";
  public static final String DA_D		=	"ad";
  public static final String DB_C		=	"bc";
  public static final String DTIMEZONE	=	"timezone";

  public static final int DATEORDER_YMD = 0;
  public static final int DATEORDER_DMY = 1;
  public static final int DATEORDER_MDY = 2;

  public static enum TokenField {
    DECIMAL(0),

    AM(0), PM(1), HR24(2),

    AD(0), BC(1),

    DTK_NUMBER(0),
    DTK_STRING(1),

    DTK_DATE(2),
    DTK_TIME(3),
    DTK_TZ(4),
    DTK_AGO(5),

    DTK_SPECIAL(6),
    DTK_INVALID(7),
    DTK_CURRENT(8),
    DTK_EARLY(9),
    DTK_LATE(10),
    DTK_EPOCH(11),
    DTK_NOW(12),
    DTK_YESTERDAY(13),
    DTK_TODAY(14),
    DTK_TOMORROW(15),
    DTK_ZULU(16),

    DTK_DELTA(17),
    DTK_SECOND(18),
    DTK_MINUTE(19),
    DTK_HOUR(20),
    DTK_DAY(21),
    DTK_WEEK(22),
    DTK_MONTH(23),
    DTK_QUARTER(24),
    DTK_YEAR(25),
    DTK_DECADE(26),
    DTK_CENTURY(27),
    DTK_MILLENNIUM(28),
    DTK_MILLISEC(29),
    DTK_MICROSEC(30),
    DTK_JULIAN(31),

    DTK_DOW(32),
    DTK_DOY(33),
    DTK_TZ_HOUR(34),
    DTK_TZ_MINUTE(35),
    DTK_ISOYEAR(36),
    DTK_ISODOW(37),

    RESERV(0),
    MONTH(1),
    YEAR(2),
    DAY(3),
    JULIAN(4),
    TZ(5),
    DTZ(6),
    DTZMOD(7),
    IGNORE_DTF(8),
    AMPM(9),
    HOUR(10),
    MINUTE(11),
    SECOND(12),
    MILLISECOND(13),
    MICROSECOND(14),
    DOY(15),
    DOW(16),
    UNITS(17),
    ADBC(18),
    /* these are only for relative dates */
    AGO(19),
    ABS_BEFORE(20),
    ABS_AFTER(21),
    /* generic fields to help with parsing */
    ISODATE(22),
    ISOTIME(23),
    /* these are only for parsing intervals */
    WEEK(24),
    DECADE(25),
    CENTURY(26),
    MILLENNIUM(27),
    /* reserved for unrecognized string values */
    UNKNOWN_FIELD(28);

    int value;
    TokenField(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  static Object datetktbl[][] = {
    /*	text, token, lexval */
    {"-infinity", TokenField.RESERV, TokenField.DTK_EARLY}, /* "-infinity" reserved for "early time" */
    {"acsst", TokenField.DTZ, 42},	/* Cent. Australia */
    {"acst", TokenField.DTZ, -16},		/* Atlantic/Porto Acre */
    {"act", TokenField.TZ, -20},		/* Atlantic/Porto Acre */
    {DA_D, TokenField.ADBC, TokenField.AD},			/* "ad" for years >= 0 */
    {"adt", TokenField.DTZ, -12},		/* Atlantic Daylight Time */
    {"aesst", TokenField.DTZ, 44},	/* E. Australia */
    {"aest", TokenField.TZ, 40},		/* Australia Eastern Std Time */
    {"aft", TokenField.TZ, 18},		/* Kabul */
    {"ahst", TokenField.TZ, -40},		/* Alaska-Hawaii Std Time */
    {"akdt", TokenField.DTZ, -32},		/* Alaska Daylight Time */
    {"akst", TokenField.DTZ, -36},		/* Alaska Standard Time */
    {"allballs", TokenField.RESERV, TokenField.DTK_ZULU},		/* 00:00:00 */
    {"almst", TokenField.TZ, 28},		/* Almaty Savings Time */
    {"almt", TokenField.TZ, 24},		/* Almaty Time */
    {"am", TokenField.AMPM, TokenField.AM},
    {"amst", TokenField.DTZ, 20},		/* Armenia Summer Time (Yerevan) */
    {"amt", TokenField.TZ, 16},		/* Armenia Time (Yerevan) */
    {"anast", TokenField.DTZ, 52},	/* Anadyr Summer Time (Russia) */
    {"anat", TokenField.TZ, 48},		/* Anadyr Time (Russia) */
    {"apr", TokenField.MONTH, 4},
    {"april", TokenField.MONTH, 4},
    {"art", TokenField.TZ, -12},		/* Argentina Time */
    {"ast", TokenField.TZ, -16},		/* Atlantic Std Time (Canada) */
    {"at", TokenField.IGNORE_DTF, 0},		/* "at" (throwaway) */
    {"aug", TokenField.MONTH, 8},
    {"august", TokenField.MONTH, 8},
    {"awsst", TokenField.DTZ, 36},	/* W. Australia */
    {"awst", TokenField.TZ, 32},		/* W. Australia */
    {"awt", TokenField.DTZ, -12},
    {"azost", TokenField.DTZ, 0},		/* Azores Summer Time */
    {"azot", TokenField.TZ, -4},		/* Azores Time */
    {"azst", TokenField.DTZ, 20},		/* Azerbaijan Summer Time */
    {"azt", TokenField.TZ, 16},		/* Azerbaijan Time */
    {DB_C, TokenField.ADBC, TokenField.BC},			/* "bc" for years < 0 */
    {"bdst", TokenField.TZ, 8},		/* British Double Summer Time */
    {"bdt", TokenField.TZ, 24},		/* Dacca */
    {"bnt", TokenField.TZ, 32},		/* Brunei Darussalam Time */
    {"bort", TokenField.TZ, 32},		/* Borneo Time (Indonesia) */
    {"bot", TokenField.TZ, -16},		/* Bolivia Time */
    {"bra", TokenField.TZ, -12},		/* Brazil Time */
    {"bst", TokenField.DTZ, 4},		/* British Summer Time */
    {"bt", TokenField.TZ, 12},		/* Baghdad Time */
    {"btt", TokenField.TZ, 24},		/* Bhutan Time */
    {"cadt", TokenField.DTZ, 42},		/* Central Australian DST */
    {"cast", TokenField.TZ, 38},		/* Central Australian ST */
    {"cat", TokenField.TZ, -40},		/* Central Alaska Time */
    {"cct", TokenField.TZ, 32},		/* China Coast Time */
    {"cdt", TokenField.DTZ, -20},		/* Central Daylight Time */
    {"cest", TokenField.DTZ, 8},		/* Central European Dayl.Time */
    {"cet", TokenField.TZ, 4},		/* Central European Time */
    {"cetdst", TokenField.DTZ, 8},	/* Central European Dayl.Time */
    {"chadt", TokenField.DTZ, 55},	/* Chatham Island Daylight Time (13:45) */
    {"chast", TokenField.TZ, 51},		/* Chatham Island Time (12:45) */
    {"ckt", TokenField.TZ, 48},		/* Cook Islands Time */
    {"clst", TokenField.DTZ, -12},		/* Chile Summer Time */
    {"clt", TokenField.TZ, -16},		/* Chile Time */
    {"cot", TokenField.TZ, -20},		/* Columbia Time */
    {"cst", TokenField.TZ, -24},		/* Central Standard Time */
    {DCURRENT, TokenField.RESERV, TokenField.DTK_CURRENT},	/* "current" is always now */
    {"cvt", TokenField.TZ, 28},		/* Christmas Island Time (Indian Ocean) */
    {"cxt", TokenField.TZ, 28},		/* Christmas Island Time (Indian Ocean) */
    {"d", TokenField.UNITS, TokenField.DTK_DAY},		/* "day of month" for ISO input */
    {"davt", TokenField.TZ, 28},		/* Davis Time (Antarctica) */
    {"ddut", TokenField.TZ, 40},		/* Dumont-d'Urville Time (Antarctica) */
    {"dec", TokenField.MONTH, 12},
    {"december", TokenField.MONTH, 12},
    {"dnt", TokenField.TZ, 4},		/* Dansk Normal Tid */
    {"dow", TokenField.RESERV, TokenField.DTK_DOW},	/* day of week */
    {"doy", TokenField.RESERV, TokenField.DTK_DOY},	/* day of year */
    {"dst", TokenField.DTZMOD, 6},
    {"easst", TokenField.DTZ, -20},	/* Easter Island Summer Time */
    {"east", TokenField.TZ, -24},		/* Easter Island Time */
    {"eat", TokenField.TZ, 12},		/* East Africa Time */
    {"edt", TokenField.DTZ, -16},		/* Eastern Daylight Time */
    {"eest", TokenField.DTZ, 12},		/* Eastern Europe Summer Time */
    {"eet", TokenField.TZ, 8},		/* East. Europe, USSR Zone 1 */
    {"eetdst", TokenField.DTZ, 12},	/* Eastern Europe Daylight Time */
    {"egst", TokenField.DTZ, 0},		/* East Greenland Summer Time */
    {"egt", TokenField.TZ, -4},		/* East Greenland Time */
    {EPOCH, TokenField.RESERV, TokenField.DTK_EPOCH}, /* "epoch" reserved for system epoch time */
    {"est", TokenField.TZ, -20},		/* Eastern Standard Time */
    {"feb", TokenField.MONTH, 2},
    {"february", TokenField.MONTH, 2},
    {"fjst", TokenField.DTZ, -52},		/* Fiji Summer Time (13 hour offset!) */
    {"fjt", TokenField.TZ, -48},		/* Fiji Time */
    {"fkst", TokenField.DTZ, -12},		/* Falkland Islands Summer Time */
    {"fkt", TokenField.TZ, -8},		/* Falkland Islands Time */
    {"fri", TokenField.DOW, 5},
    {"friday", TokenField.DOW, 5},
    {"fst", TokenField.TZ, 4},		/* French Summer Time */
    {"fwt", TokenField.DTZ, 8},		/* French Winter Time  */
    {"galt", TokenField.TZ, -24},		/* Galapagos Time */
    {"gamt", TokenField.TZ, -36},		/* Gambier Time */
    {"gest", TokenField.DTZ, 20},		/* Georgia Summer Time */
    {"get", TokenField.TZ, 16},		/* Georgia Time */
    {"gft", TokenField.TZ, -12},		/* French Guiana Time */
    {"gilt", TokenField.TZ, 48},		/* Gilbert Islands Time */
    {"gmt", TokenField.TZ, 0},		/* Greenwish Mean Time */
    {"gst", TokenField.TZ, 40},		/* Guam Std Time, USSR Zone 9 */
    {"gyt", TokenField.TZ, -16},		/* Guyana Time */
    {"h", TokenField.UNITS, TokenField.DTK_HOUR},		/* "hour" */
    {"hdt", TokenField.DTZ, -36},		/* Hawaii/Alaska Daylight Time */
    {"hkt", TokenField.TZ, 32},		/* Hong Kong Time */
    {"hst", TokenField.TZ, -40},		/* Hawaii Std Time */
    {"ict", TokenField.TZ, 28},		/* Indochina Time */
    {"idle", TokenField.TZ, 48},		/* Intl. Date Line, East */
    {"idlw", TokenField.TZ, -48},		/* Intl. Date Line, West */
    {LATE, TokenField.RESERV, TokenField.DTK_LATE},	/* "infinity" reserved for "late time" */
    {INVALID, TokenField.RESERV, TokenField.DTK_INVALID},		/* "invalid" reserved for bad time */
    {"iot", TokenField.TZ, 20},		/* Indian Chagos Time */
    {"irkst", TokenField.DTZ, 36},	/* Irkutsk Summer Time */
    {"irkt", TokenField.TZ, 32},		/* Irkutsk Time */
    {"irt", TokenField.TZ, 14},		/* Iran Time */
    {"isodow", TokenField.RESERV, TokenField.DTK_ISODOW},		/* ISO day of week, Sunday == 7 */
    {"ist", TokenField.TZ, 8},		/* Israel */
    {"it", TokenField.TZ, 14},		/* Iran Time */
    {"j", TokenField.UNITS, TokenField.DTK_JULIAN},
    {"jan", TokenField.MONTH, 1},
    {"january", TokenField.MONTH, 1},
    {"javt", TokenField.TZ, 28},		/* Java Time (07:00? see JT) */
    {"jayt", TokenField.TZ, 36},		/* Jayapura Time (Indonesia) */
    {"jd", TokenField.UNITS, TokenField.DTK_JULIAN},
    {"jst", TokenField.TZ, 36},		/* Japan Std Time,USSR Zone 8 */
    {"jt", TokenField.TZ, 30},		/* Java Time (07:30? see JAVT) */
    {"jul", TokenField.MONTH, 7},
    {"julian", TokenField.UNITS, TokenField.DTK_JULIAN},
    {"july", TokenField.MONTH, 7},
    {"jun", TokenField.MONTH, 6},
    {"june", TokenField.MONTH, 6},
    {"kdt", TokenField.DTZ, 40},		/* Korea Daylight Time */
    {"kgst", TokenField.DTZ, 24},		/* Kyrgyzstan Summer Time */
    {"kgt", TokenField.TZ, 20},		/* Kyrgyzstan Time */
    {"kost", TokenField.TZ, 48},		/* Kosrae Time */
    {"krast", TokenField.DTZ, 28},	/* Krasnoyarsk Summer Time */
    {"krat", TokenField.TZ, 32},		/* Krasnoyarsk Standard Time */
    {"kst", TokenField.TZ, 36},		/* Korea Standard Time */
    {"lhdt", TokenField.DTZ, 44},		/* Lord Howe Daylight Time, Australia */
    {"lhst", TokenField.TZ, 42},		/* Lord Howe Standard Time, Australia */
    {"ligt", TokenField.TZ, 40},		/* From Melbourne, Australia */
    {"lint", TokenField.TZ, 56},		/* Line Islands Time (Kiribati; +14 hours!) */
    {"lkt", TokenField.TZ, 24},		/* Lanka Time */
    {"m", TokenField.UNITS, TokenField.DTK_MONTH},	/* "month" for ISO input */
    {"magst", TokenField.DTZ, 48},	/* Magadan Summer Time */
    {"magt", TokenField.TZ, 44},		/* Magadan Time */
    {"mar", TokenField.MONTH, 3},
    {"march", TokenField.MONTH, 3},
    {"mart", TokenField.TZ, -38},		/* Marquesas Time */
    {"mawt", TokenField.TZ, 24},		/* Mawson, Antarctica */
    {"may", TokenField.MONTH, 5},
    {"mdt", TokenField.DTZ, -24},		/* Mountain Daylight Time */
    {"mest", TokenField.DTZ, 8},		/* Middle Europe Summer Time */
    {"met", TokenField.TZ, 4},		/* Middle Europe Time */
    {"metdst", TokenField.DTZ, 8},	/* Middle Europe Daylight Time */
    {"mewt", TokenField.TZ, 4},		/* Middle Europe Winter Time */
    {"mez", TokenField.TZ, 4},		/* Middle Europe Zone */
    {"mht", TokenField.TZ, 48},		/* Kwajalein */
    {"mm", TokenField.UNITS, TokenField.DTK_MINUTE},	/* "minute" for ISO input */
    {"mmt", TokenField.TZ, 26},		/* Myannar Time */
    {"mon", TokenField.DOW, 1},
    {"monday", TokenField.DOW, 1},
    {"mpt", TokenField.TZ, 40},		/* North Mariana Islands Time */
    {"msd", TokenField.DTZ, 16},		/* Moscow Summer Time */
    {"msk", TokenField.TZ, 12},		/* Moscow Time */
    {"mst", TokenField.TZ, -28},		/* Mountain Standard Time */
    {"mt", TokenField.TZ, 34},		/* Moluccas Time */
    {"mut", TokenField.TZ, 16},		/* Mauritius Island Time */
    {"mvt", TokenField.TZ, 20},		/* Maldives Island Time */
    {"myt", TokenField.TZ, 32},		/* Malaysia Time */
    {"nct", TokenField.TZ, 44},		/* New Caledonia Time */
    {"ndt", TokenField.DTZ, -10},		/* Nfld. Daylight Time */
    {"nft", TokenField.TZ, -14},		/* Newfoundland Standard Time */
    {"nor", TokenField.TZ, 4},		/* Norway Standard Time */
    {"nov", TokenField.MONTH, 11},
    {"november", TokenField.MONTH, 11},
    {"novst", TokenField.DTZ, 28},	/* Novosibirsk Summer Time */
    {"novt", TokenField.TZ, 24},		/* Novosibirsk Standard Time */
    {NOW, TokenField.RESERV, TokenField.DTK_NOW},		/* current transaction time */
    {"npt", TokenField.TZ, 23},		/* Nepal Standard Time (GMT-5:45) */
    {"nst", TokenField.TZ, -14},		/* Nfld. Standard Time */
    {"nt", TokenField.TZ, -44},		/* Nome Time */
    {"nut", TokenField.TZ, -44},		/* Niue Time */
    {"nzdt", TokenField.DTZ, 52},		/* New Zealand Daylight Time */
    {"nzst", TokenField.TZ, 48},		/* New Zealand Standard Time */
    {"nzt", TokenField.TZ, 48},		/* New Zealand Time */
    {"oct", TokenField.MONTH, 10},
    {"october", TokenField.MONTH, 10},
    {"omsst", TokenField.DTZ, 28},	/* Omsk Summer Time */
    {"omst", TokenField.TZ, 24},		/* Omsk Time */
    {"on", TokenField.IGNORE_DTF, 0},		/* "on" (throwaway) */
    {"pdt", TokenField.DTZ, -28},		/* Pacific Daylight Time */
    {"pet", TokenField.TZ, -20},		/* Peru Time */
    {"petst", TokenField.DTZ, 52},	/* Petropavlovsk-Kamchatski Summer Time */
    {"pett", TokenField.TZ, 48},		/* Petropavlovsk-Kamchatski Time */
    {"pgt", TokenField.TZ, 40},		/* Papua New Guinea Time */
    {"phot", TokenField.TZ, 52},		/* Phoenix Islands (Kiribati) Time */
    {"pht", TokenField.TZ, 32},		/* Philippine Time */
    {"pkt", TokenField.TZ, 20},		/* Pakistan Time */
    {"pm", TokenField.AMPM, TokenField.PM},
    {"pmdt", TokenField.DTZ, -8},		/* Pierre & Miquelon Daylight Time */
    {"pont", TokenField.TZ, 44},		/* Ponape Time (Micronesia) */
    {"pst", TokenField.TZ, -32},		/* Pacific Standard Time */
    {"pwt", TokenField.TZ, 36},		/* Palau Time */
    {"pyst", TokenField.DTZ, -12},		/* Paraguay Summer Time */
    {"pyt", TokenField.TZ, -16},		/* Paraguay Time */
    {"ret", TokenField.DTZ, 16},		/* Reunion Island Time */
    {"s", TokenField.UNITS, TokenField.DTK_SECOND},	/* "seconds" for ISO input */
    {"sadt", TokenField.DTZ, 42},		/* S. Australian Dayl. Time */
    {"sast", TokenField.TZ, 38},		/* South Australian Std Time */
    {"sat", TokenField.DOW, 6},
    {"saturday", TokenField.DOW, 6},
    {"sct", TokenField.DTZ, 16},		/* Mahe Island Time */
    {"sep", TokenField.MONTH, 9},
    {"sept", TokenField.MONTH, 9},
    {"september", TokenField.MONTH, 9},
    {"set", TokenField.TZ, -4},		/* Seychelles Time ?? */
    {"sst", TokenField.DTZ, 8},		/* Swedish Summer Time */
    {"sun", TokenField.DOW, 0},
    {"sunday", TokenField.DOW, 0},
    {"swt", TokenField.TZ, 4},		/* Swedish Winter Time */
    {"t", TokenField.ISOTIME, TokenField.DTK_TIME},	/* Filler for ISO time fields */
    {"tft", TokenField.TZ, 20},		/* Kerguelen Time */
    {"that", TokenField.TZ, -40},		/* Tahiti Time */
    {"thu", TokenField.DOW, 4},
    {"thur", TokenField.DOW, 4},
    {"thurs", TokenField.DOW, 4},
    {"thursday", TokenField.DOW, 4},
    {"tjt", TokenField.TZ, 20},		/* Tajikistan Time */
    {"tkt", TokenField.TZ, -40},		/* Tokelau Time */
    {"tmt", TokenField.TZ, 20},		/* Turkmenistan Time */
    {TODAY, TokenField.RESERV, TokenField.DTK_TODAY}, /* midnight */
    {TOMORROW, TokenField.RESERV, TokenField.DTK_TOMORROW},	/* tomorrow midnight */
    {"truk", TokenField.TZ, 40},		/* Truk Time */
    {"tue", TokenField.DOW, 2},
    {"tues", TokenField.DOW, 2},
    {"tuesday", TokenField.DOW, 2},
    {"tvt", TokenField.TZ, 48},		/* Tuvalu Time */
    {"ulast", TokenField.DTZ, 36},	/* Ulan Bator Summer Time */
    {"ulat", TokenField.TZ, 32},		/* Ulan Bator Time */
    {"undefined", TokenField.RESERV, TokenField.DTK_INVALID}, /* pre-v6.1 invalid time */
    {"ut", TokenField.TZ, 0},
    {"utc", TokenField.TZ, 0},
    {"uyst", TokenField.DTZ, -8},		/* Uruguay Summer Time */
    {"uyt", TokenField.TZ, -12},		/* Uruguay Time */
    {"uzst", TokenField.DTZ, 24},		/* Uzbekistan Summer Time */
    {"uzt", TokenField.TZ, 20},		/* Uzbekistan Time */
    {"vet", TokenField.TZ, -16},		/* Venezuela Time */
    {"vlast", TokenField.DTZ, 44},	/* Vladivostok Summer Time */
    {"vlat", TokenField.TZ, 40},		/* Vladivostok Time */
    {"vut", TokenField.TZ, 44},		/* Vanuata Time */
    {"wadt", TokenField.DTZ, 32},		/* West Australian DST */
    {"wakt", TokenField.TZ, 48},		/* Wake Time */
    {"wast", TokenField.TZ, 28},		/* West Australian Std Time */
    {"wat", TokenField.TZ, -4},		/* West Africa Time */
    {"wdt", TokenField.DTZ, 36},		/* West Australian DST */
    {"wed", TokenField.DOW, 3},
    {"wednesday", TokenField.DOW, 3},
    {"weds", TokenField.DOW, 3},
    {"west", TokenField.DTZ, 4},		/* Western Europe Summer Time */
    {"wet", TokenField.TZ, 0},		/* Western Europe */
    {"wetdst", TokenField.DTZ, 4},	/* Western Europe Daylight Savings Time */
    {"wft", TokenField.TZ, 48},		/* Wallis and Futuna Time */
    {"wgst", TokenField.DTZ, -8},		/* West Greenland Summer Time */
    {"wgt", TokenField.TZ, -12},		/* West Greenland Time */
    {"wst", TokenField.TZ, 32},		/* West Australian Standard Time */
    {"y", TokenField.UNITS, TokenField.DTK_YEAR},		/* "year" for ISO input */
    {"yakst", TokenField.DTZ, 40},	/* Yakutsk Summer Time */
    {"yakt", TokenField.TZ, 36},		/* Yakutsk Time */
    {"yapt", TokenField.TZ, 40},		/* Yap Time (Micronesia) */
    {"ydt", TokenField.DTZ, -32},		/* Yukon Daylight Time */
    {"yekst", TokenField.DTZ, 24},	/* Yekaterinburg Summer Time */
    {"yekt", TokenField.TZ, 20},		/* Yekaterinburg Time */
    {YESTERDAY, TokenField.RESERV, TokenField.DTK_YESTERDAY}, /* yesterday midnight */
    {"yst", TokenField.TZ, -36},		/* Yukon Standard Time */
    {"z", TokenField.TZ, 0},			/* time zone tag per ISO-8601 */
    {"zp4", TokenField.TZ, -16},		/* UTC +4  hours. */
    {"zp5", TokenField.TZ, -20},		/* UTC +5  hours. */
    {"zp6", TokenField.TZ, -24},		/* UTC +6  hours. */
    {ZULU, TokenField.TZ, 0},			/* UTC */
  };

  static Object[][] deltatktbl = {
	  /* text, token, lexval */
    {"@", TokenField.IGNORE_DTF, 0},		/* postgres relative prefix */
    {DAGO, TokenField.AGO, 0},				/* "ago" indicates negative time offset */
    {"c", TokenField.UNITS, TokenField.DTK_CENTURY},	/* "century" relative */
    {"cent", TokenField.UNITS, TokenField.DTK_CENTURY},		/* "century" relative */
    {"centuries", TokenField.UNITS, TokenField.DTK_CENTURY},	/* "centuries" relative */
    {DCENTURY, TokenField.UNITS, TokenField.DTK_CENTURY},		/* "century" relative */
    {"d", TokenField.UNITS, TokenField.DTK_DAY},		/* "day" relative */
    {DDAY, TokenField.UNITS, TokenField.DTK_DAY},		/* "day" relative */
    {"days_full", TokenField.UNITS, TokenField.DTK_DAY},	/* "days_full" relative */
    {"dec", TokenField.UNITS, TokenField.DTK_DECADE}, /* "decade" relative */
    {DDECADE, TokenField.UNITS, TokenField.DTK_DECADE},		/* "decade" relative */
    {"decades", TokenField.UNITS, TokenField.DTK_DECADE},		/* "decades" relative */
    {"decs", TokenField.UNITS, TokenField.DTK_DECADE},	/* "decades" relative */
    {"h", TokenField.UNITS, TokenField.DTK_HOUR},		/* "hour" relative */
    {DHOUR, TokenField.UNITS, TokenField.DTK_HOUR},	/* "hour" relative */
    {"hours", TokenField.UNITS, TokenField.DTK_HOUR}, /* "hours" relative */
    {"hr", TokenField.UNITS, TokenField.DTK_HOUR},	/* "hour" relative */
    {"hrs", TokenField.UNITS, TokenField.DTK_HOUR},	/* "hours" relative */
    {INVALID, TokenField.RESERV, TokenField.DTK_INVALID},		/* reserved for invalid time */
    {"m", TokenField.UNITS, TokenField.DTK_MINUTE},	/* "minute" relative */
    {"microsecon", TokenField.UNITS, TokenField.DTK_MICROSEC},		/* "microsecond" relative */
    {"mil", TokenField.UNITS, TokenField.DTK_MILLENNIUM},		/* "millennium" relative */
    {"millennia", TokenField.UNITS, TokenField.DTK_MILLENNIUM},		/* "millennia" relative */
    {DMILLENNIUM, TokenField.UNITS, TokenField.DTK_MILLENNIUM},		/* "millennium" relative */
    {"millisecon", TokenField.UNITS, TokenField.DTK_MILLISEC},		/* relative */
    {"mils", TokenField.UNITS, TokenField.DTK_MILLENNIUM},	/* "millennia" relative */
    {"min", TokenField.UNITS, TokenField.DTK_MINUTE}, /* "minute" relative */
    {"mins", TokenField.UNITS, TokenField.DTK_MINUTE},	/* "minutes" relative */
    {DMINUTE, TokenField.UNITS, TokenField.DTK_MINUTE},		/* "minute" relative */
    {"minutes", TokenField.UNITS, TokenField.DTK_MINUTE},		/* "minutes" relative */
    {"mon", TokenField.UNITS, TokenField.DTK_MONTH},	/* "months_short" relative */
    {"mons", TokenField.UNITS, TokenField.DTK_MONTH}, /* "months_short" relative */
    {DMONTH, TokenField.UNITS, TokenField.DTK_MONTH}, /* "month" relative */
    {"months_short", TokenField.UNITS, TokenField.DTK_MONTH},
    {"ms", TokenField.UNITS, TokenField.DTK_MILLISEC},
    {"msec", TokenField.UNITS, TokenField.DTK_MILLISEC},
    {DMILLISEC, TokenField.UNITS, TokenField.DTK_MILLISEC},
    {"mseconds", TokenField.UNITS, TokenField.DTK_MILLISEC},
    {"msecs", TokenField.UNITS, TokenField.DTK_MILLISEC},
    {"qtr", TokenField.UNITS, TokenField.DTK_QUARTER},	/* "quarter" relative */
    {DQUARTER, TokenField.UNITS, TokenField.DTK_QUARTER},		/* "quarter" relative */
    {"s", TokenField.UNITS, TokenField.DTK_SECOND},
    {"sec", TokenField.UNITS, TokenField.DTK_SECOND},
    {DSECOND, TokenField.UNITS, TokenField.DTK_SECOND},
    {"seconds", TokenField.UNITS, TokenField.DTK_SECOND},
    {"secs", TokenField.UNITS, TokenField.DTK_SECOND},
    {DTIMEZONE, TokenField.UNITS, TokenField.DTK_TZ}, /* "timezone" time offset */
    {"timezone_h", TokenField.UNITS, TokenField.DTK_TZ_HOUR}, /* timezone hour units */
    {"timezone_m", TokenField.UNITS, TokenField.DTK_TZ_MINUTE},		/* timezone minutes units */
    {"undefined", TokenField.RESERV, TokenField.DTK_INVALID}, /* pre-v6.1 invalid time */
    {"us", TokenField.UNITS, TokenField.DTK_MICROSEC},	/* "microsecond" relative */
    {"usec", TokenField.UNITS, TokenField.DTK_MICROSEC},		/* "microsecond" relative */
    {DMICROSEC, TokenField.UNITS, TokenField.DTK_MICROSEC},	/* "microsecond" relative */
    {"useconds", TokenField.UNITS, TokenField.DTK_MICROSEC},	/* "microseconds" relative */
    {"usecs", TokenField.UNITS, TokenField.DTK_MICROSEC},		/* "microseconds" relative */
    {"w", TokenField.UNITS, TokenField.DTK_WEEK},		/* "week" relative */
    {DWEEK, TokenField.UNITS, TokenField.DTK_WEEK},	/* "week" relative */
    {"weeks", TokenField.UNITS, TokenField.DTK_WEEK}, /* "weeks" relative */
    {"y", TokenField.UNITS, TokenField.DTK_YEAR},		/* "year" relative */
    {DYEAR, TokenField.UNITS, TokenField.DTK_YEAR},	/* "year" relative */
    {"years", TokenField.UNITS, TokenField.DTK_YEAR}, /* "years" relative */
    {"yr", TokenField.UNITS, TokenField.DTK_YEAR},	/* "year" relative */
    {"yrs", TokenField.UNITS, TokenField.DTK_YEAR},	/* "years" relative */
  };

  public static class DateToken {
    String key;
    TokenField type;
    int value;
    TokenField valueType;

    public String getKey() {
      return key;
    }

    public TokenField getType() {
      return type;
    }

    public int getValue() {
      return value;
    }

    public TokenField getValueType() {
      return valueType;
    }
  }
  public static final Map<String, DateToken> dateTokenMap = new HashMap<>();

  static {
    for (Object[] eachToken: datetktbl) {
      DateToken dateToken = new DateToken();
      dateToken.key = eachToken[0].toString();
      dateToken.type = (TokenField)eachToken[1];
      if (eachToken[2] instanceof TokenField) {
        dateToken.valueType = (TokenField)eachToken[2];
        dateToken.value = dateToken.valueType.getValue();
      } else {
        dateToken.valueType = TokenField.DECIMAL;
        dateToken.value = ((Integer)eachToken[2]).intValue();
      }
      dateTokenMap.put(dateToken.key, dateToken);
    }

    /*
     * TODO
     * Currently, tajo does not support intervals, yet.
     * The below code must be restored to support intervals.
     */
//    for (Object[] eachToken: deltatktbl) {
//      DateToken dateToken = new DateToken();
//      dateToken.key = eachToken[0].toString();
//      dateToken.type = (TokenField)eachToken[1];
//      if (eachToken[2] instanceof TokenField) {
//        dateToken.valueType = (TokenField)eachToken[2];
//        dateToken.value = dateToken.valueType.getValue();
//      } else {
//        dateToken.valueType = TokenField.DECIMAL;
//        dateToken.value = ((Integer)eachToken[2]).intValue();
//      }
//      dateTokenMap.put(dateToken.key, dateToken);
//    }
  }

  public static int INTERVAL_MASK(TokenField t) { return (1 << (t.getValue())); }
  public static int DTK_M(TokenField t) { return (0x01 << (t.getValue())); }

  public static final int DTK_ALL_SECS_M = (DTK_M(TokenField.SECOND) |
        DTK_M(TokenField.MILLISECOND) |
        DTK_M(TokenField.MICROSECOND));
  public static final int DTK_DATE_M = (DTK_M(TokenField.YEAR) | DTK_M(TokenField.MONTH) | DTK_M(TokenField.DAY));
  public static final int DTK_TIME_M = (DTK_M(TokenField.HOUR) | DTK_M(TokenField.MINUTE) | DTK_M(TokenField.SECOND));
}
