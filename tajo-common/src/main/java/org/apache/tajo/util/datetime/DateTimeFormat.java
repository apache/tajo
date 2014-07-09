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

import org.apache.tajo.datum.TimestampDatum;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class originated from src/backend/utils/adt/formatting.c of PostgreSQL
 */
public class DateTimeFormat {
  /* ----------
   * Full months_short
   * ----------
   */
  static final String[] months_full = {
    "January", "February", "March", "April", "May", "June", "July",
        "August", "September", "October", "November", "December", null
  };

  static String[] days_short = {
    "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", null
  };

  static String[] months_short = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", null};

  static String[] days_full = {"Sunday", "Monday", "Tuesday", "Wednesday",
      "Thursday", "Friday", "Saturday", null};

  static int[][] ysum = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366}
  };


  /**
   *  AD / BC
   * ----------
   * There is no 0 AD.  Years go from 1 BC to 1 AD, so we make it
   * positive and map year == -1 to year zero, and shift all negative
   * years up one.  For interval years, we just return the year.
   * @param year
   * @param is_interval
   * @return
   */
  static int ADJUST_YEAR(int year, boolean is_interval)	{
    return ((is_interval) ? (year) : ((year) <= 0 ? -((year) - 1) : (year)));
  }

  static final String A_D_STR	= "A.D.";
  static final String a_d_STR	= "a.d.";
  static final String AD_STR = "AD";
  static final String ad_STR = "ad";

  static final String B_C_STR	= "B.C.";
  static final String b_c_STR	= "b.c.";
  static final String BC_STR = "BC";
  static final String bc_STR = "bc";

  /**
   * AD / BC strings for seq_search.
   *
   * These are given in two variants, a long form with periods and a standard
   * form without.
   *
   * The array is laid out such that matches for AD have an even index, and
   * matches for BC have an odd index.  So the boolean value for BC is given by
   * taking the array index of the match, modulo 2.
   */
  static final String[] adbc_strings = {ad_STR, bc_STR, AD_STR, BC_STR, null};
  static final String[] adbc_strings_long = {a_d_STR, b_c_STR, A_D_STR, B_C_STR, null};

  /**
   * ----------
   * AM / PM
   * ----------
   */
  static final String A_M_STR	= "A.M.";
  static final String a_m_STR	= "a.m.";
  static final String AM_STR = "AM";
  static final String am_STR = "am";

  static final String P_M_STR	= "P.M.";
  static final String p_m_STR	= "p.m.";
  static final String PM_STR = "PM";
  static final String pm_STR = "pm";

  /**
   * AM / PM strings for seq_search.
   *
   * These are given in two variants, a long form with periods and a standard
   * form without.
   *
   * The array is laid out such that matches for AM have an even index, and
   * matches for PM have an odd index.  So the boolean value for PM is given by
   * taking the array index of the match, modulo 2.
   */
  static final String[] ampm_strings = {am_STR, pm_STR, AM_STR, PM_STR, null};
  static final String[] ampm_strings_long = {a_m_STR, p_m_STR, A_M_STR, P_M_STR, null};

  /**
   * ----------
   * Months in roman-numeral
   * (Must be in reverse order for seq_search (in FROM_CHAR), because
   *	'VIII' must have higher precedence than 'V')
   * ----------
   */
  static final String[] rm_months_upper =
    {"XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I", null};

  static final String[] rm_months_lower =
    {"xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i", null};

  /**
   * ----------
   * Roman numbers
   * ----------
   */
  static final String[] rm1 = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", null};
  static final String[] rm10 = {"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC", null};
  static final String[] rm100 = {"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM", null};

  /**
   * ----------
   * Ordinal postfixes
   * ----------
   */
  static final String[] numTH = {"ST", "ND", "RD", "TH", null};
  static final String[] numth = {"st", "nd", "rd", "th", null};

  /**
   * ----------
   * Flags & Options:
   * ----------
   */
  static final int ONE_UPPER = 1;		/* Name */
  static final int ALL_UPPER = 2;		/* NAME */
  static final int ALL_LOWER = 3;		/* name */

  static final int MAX_MONTH_LEN = 9;
  static final int MAX_MON_LEN = 3;
  static final int MAX_DAY_LEN = 9;
  static final int MAX_DY_LEN	= 3;
  static final int MAX_RM_LEN	= 4;

  static final int DCH_S_FM =	0x01;
  static final int DCH_S_TH	= 0x02;
  static final int DCH_S_th	= 0x04;
  static final int DCH_S_SP	= 0x08;
  static final int DCH_S_TM	= 0x10;

  static final int NODE_TYPE_END = 1;
  static final int NODE_TYPE_ACTION = 2;
  static final int NODE_TYPE_CHAR = 3;

  static final int SUFFTYPE_PREFIX = 1;
  static final int SUFFTYPE_POSTFIX = 2;

  static final int CLOCK_24_HOUR = 0;
  static final int CLOCK_12_HOUR = 1;

  static final int MONTHS_PER_YEAR = 12;
  static final int HOURS_PER_DAY = 24;

  /**
   * ----------
   * Maximal length of one node
   * ----------
   */
  static final int DCH_MAX_ITEM_SIZ	= 9;		/* max julian day		*/
  static final int NUM_MAX_ITEM_SIZ	= 8;		/* roman number (RN has 15 chars)	*/

  enum FORMAT_TYPE {
    DCH_TYPE, NUM_TYPE
  }

  /**
   * ----------
   * Suffixes definition for DATE-TIME TO/FROM CHAR
   * ----------
   */
  static KeySuffix[] DCH_suff = {
    new KeySuffix("FM", 2, DCH_S_FM, SUFFTYPE_PREFIX),
    new KeySuffix("fm", 2, DCH_S_FM, SUFFTYPE_PREFIX),
    new KeySuffix("TM", 2, DCH_S_TM, SUFFTYPE_PREFIX),
    new KeySuffix("tm", 2, DCH_S_TM, SUFFTYPE_PREFIX),
    new KeySuffix("TH", 2, DCH_S_TH, SUFFTYPE_POSTFIX),
    new KeySuffix("th", 2, DCH_S_th, SUFFTYPE_POSTFIX),
    new KeySuffix("SP", 2, DCH_S_SP, SUFFTYPE_POSTFIX),
  };

  /**
   * ----------
   * Format-pictures (KeyWord).
   *
   * The KeyWord field; alphabetic sorted, *BUT* strings alike is sorted
   *		  complicated -to-> easy:
   *
   *	(example: "DDD","DD","Day","D" )
   *
   * (this specific sort needs the algorithm for sequential search for strings,
   * which not has exact end; -> How keyword is in "HH12blabla" ? - "HH"
   * or "HH12"? You must first try "HH12", because "HH" is in string, but
   * it is not good.
   *
   * (!)
   *	 - Position for the keyword is similar as position in the enum DCH/NUM_poz.
   * (!)
   *
   * For fast search is used the 'int index[]', index is ascii table from position
   * 32 (' ') to 126 (~), in this index is DCH_ / NUM_ enums for each ASCII
   * position or -1 if char is not used in the KeyWord. Search example for
   * string "MM":
   *	1)	see in index to index['M' - 32],
   *	2)	take keywords position (enum DCH_MI) from index
   *	3)	run sequential search in keywords[] from this position
   *
   * ----------
   */
  enum DCH_poz {
    DCH_A_D(0),
    DCH_A_M(1),
    DCH_AD(2),
    DCH_AM(3),
    DCH_B_C(4),
    DCH_BC(5),
    DCH_CC(6),
    DCH_DAY(7),
    DCH_DDD(8),
    DCH_DD(9),

    DCH_DY(10),
    DCH_Day(11),
    DCH_Dy(12),
    DCH_D(13),
    DCH_FX(14),						/* global suffix */
    DCH_HH24(15),
    DCH_HH12(16),
    DCH_HH(17),
    DCH_IDDD(18),
    DCH_ID(19),

    DCH_IW(20),
    DCH_IYYY(21),
    DCH_IYY(22),
    DCH_IY(23),
    DCH_I(24),
    DCH_J(25),
    DCH_MI(26),
    DCH_MM(27),
    DCH_MONTH(28),
    DCH_MON(29),

    DCH_MS(30),
    DCH_Month(31),
    DCH_Mon(32),
    DCH_P_M(33),
    DCH_PM(34),
    DCH_Q(35),
    DCH_RM(36),
    DCH_SSSS(37),
    DCH_SS(38),
    DCH_TZ(39),

    DCH_US(40),
    DCH_WW(41),
    DCH_W(42),
    DCH_Y_YYY(43),
    DCH_YYYY(44),
    DCH_YYY(45),
    DCH_YY(46),
    DCH_Y(47),
    DCH_a_d(48),
    DCH_a_m(49),

    DCH_ad(50),
    DCH_am(51),
    DCH_b_c(52),
    DCH_bc(53),
    DCH_cc(54),
    DCH_day(55),
    DCH_ddd(56),
    DCH_dd(57),
    DCH_dy(58),
    DCH_d(59),

    DCH_fx(60),
    DCH_hh24(61),
    DCH_hh12(62),
    DCH_hh(63),
    DCH_iddd(64),
    DCH_id(65),
    DCH_iw(66),
    DCH_iyyy(67),
    DCH_iyy(68),
    DCH_iy(69),

    DCH_i(70),
    DCH_j(71),
    DCH_mi(72),
    DCH_mm(73),
    DCH_month(74),
    DCH_mon(75),
    DCH_ms(76),
    DCH_p_m(77),
    DCH_pm(78),
    DCH_q(79),

    DCH_rm(80),
    DCH_ssss(89),
    DCH_ss(90),
    DCH_tz(91),
    DCH_us(92),
    DCH_ww(93),
    DCH_w(94),
    DCH_y_yyy(95),
    DCH_yyyy(96),
    DCH_yyy(97),
    DCH_yy(98),
    DCH_y(99),
    _DCH_last_(Integer.MAX_VALUE);

    int value;
    DCH_poz(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * ----------
   * FromCharDateMode
   * ----------
   *
   * This value is used to nominate one of several distinct (and mutually
   * exclusive) date conventions that a keyword can belong to.
   */
  enum FromCharDateMode
  {
    FROM_CHAR_DATE_NONE,	/* Value does not affect date mode. */
    FROM_CHAR_DATE_GREGORIAN,	/* Gregorian (day, month, year) style date */
    FROM_CHAR_DATE_ISOWEEK		/* ISO 8601 week date */
  }

  /**
   * ----------
   * KeyWords for DATE-TIME version
   * ----------
   */
  static final Object[][] DCH_keywordValues = {
      /*	name, len, id, is_digit, date_mode */
      {"A.D.", 4, DCH_poz.DCH_A_D, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* A */
      {"A.M.", 4, DCH_poz.DCH_A_M, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"AD", 2, DCH_poz.DCH_AD, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"AM", 2, DCH_poz.DCH_AM, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"B.C.", 4, DCH_poz.DCH_B_C, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* B */
      {"BC", 2, DCH_poz.DCH_BC, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"CC", 2, DCH_poz.DCH_CC, true, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* C */
      {"DAY", 3, DCH_poz.DCH_DAY, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* D */
      {"DDD", 3, DCH_poz.DCH_DDD, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"DD", 2, DCH_poz.DCH_DD, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"DY", 2, DCH_poz.DCH_DY, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"Day", 3, DCH_poz.DCH_Day, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"Dy", 2, DCH_poz.DCH_Dy, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"D", 1, DCH_poz.DCH_D, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"FX", 2, DCH_poz.DCH_FX, false, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* F */
      {"HH24", 4, DCH_poz.DCH_HH24, true, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* H */
      {"HH12", 4, DCH_poz.DCH_HH12, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"HH", 2, DCH_poz.DCH_HH, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"IDDD", 4, DCH_poz.DCH_IDDD, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},		/* I */
      {"ID", 2, DCH_poz.DCH_ID, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"IW", 2, DCH_poz.DCH_IW, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"IYYY", 4, DCH_poz.DCH_IYYY, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"IYY", 3, DCH_poz.DCH_IYY, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"IY", 2, DCH_poz.DCH_IY, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"I", 1, DCH_poz.DCH_I, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"J", 1, DCH_poz.DCH_J, true, FromCharDateMode.FROM_CHAR_DATE_NONE}, /* J */
      {"MI", 2, DCH_poz.DCH_MI, true, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* M */
      {"MM", 2, DCH_poz.DCH_MM, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"MONTH", 5, DCH_poz.DCH_MONTH, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"MON", 3, DCH_poz.DCH_MON, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"MS", 2, DCH_poz.DCH_MS, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"Month", 5, DCH_poz.DCH_Month, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"Mon", 3, DCH_poz.DCH_Mon, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"P.M.", 4, DCH_poz.DCH_P_M, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* P */
      {"PM", 2, DCH_poz.DCH_PM, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"Q", 1, DCH_poz.DCH_Q, true, FromCharDateMode.FROM_CHAR_DATE_NONE}, /* Q */
      {"RM", 2, DCH_poz.DCH_RM, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN}, /* R */
      {"SSSS", 4, DCH_poz.DCH_SSSS, true, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* S */
      {"SS", 2, DCH_poz.DCH_SS, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"TZ", 2, DCH_poz.DCH_TZ, false, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* T */
      {"US", 2, DCH_poz.DCH_US, true, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* U */
      {"WW", 2, DCH_poz.DCH_WW, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},	/* W */
      {"W", 1, DCH_poz.DCH_W, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"Y,YYY", 5, DCH_poz.DCH_Y_YYY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},	/* Y */
      {"YYYY", 4, DCH_poz.DCH_YYYY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"YYY", 3, DCH_poz.DCH_YYY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"YY", 2, DCH_poz.DCH_YY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"Y", 1, DCH_poz.DCH_Y, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"a.d.", 4, DCH_poz.DCH_a_d, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* a */
      {"a.m.", 4, DCH_poz.DCH_a_m, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"ad", 2, DCH_poz.DCH_ad, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"am", 2, DCH_poz.DCH_am, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"b.c.", 4, DCH_poz.DCH_b_c, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* b */
      {"bc", 2, DCH_poz.DCH_bc, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"cc", 2, DCH_poz.DCH_CC, true, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* c */
      {"day", 3, DCH_poz.DCH_day, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* d */
      {"ddd", 3, DCH_poz.DCH_DDD, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"dd", 2, DCH_poz.DCH_DD, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"dy", 2, DCH_poz.DCH_dy, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"d", 1, DCH_poz.DCH_D, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"fx", 2, DCH_poz.DCH_FX, false, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* f */
      {"hh24", 4, DCH_poz.DCH_HH24, true, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* h */
      {"hh12", 4, DCH_poz.DCH_HH12, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"hh", 2, DCH_poz.DCH_HH, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"iddd", 4, DCH_poz.DCH_IDDD, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},		/* i */
      {"id", 2, DCH_poz.DCH_ID, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"iw", 2, DCH_poz.DCH_IW, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"iyyy", 4, DCH_poz.DCH_IYYY, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"iyy", 3, DCH_poz.DCH_IYY, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"iy", 2, DCH_poz.DCH_IY, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"i", 1, DCH_poz.DCH_I, true, FromCharDateMode.FROM_CHAR_DATE_ISOWEEK},
      {"j", 1, DCH_poz.DCH_J, true, FromCharDateMode.FROM_CHAR_DATE_NONE}, /* j */
      {"mi", 2, DCH_poz.DCH_MI, true, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* m */
      {"mm", 2, DCH_poz.DCH_MM, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"month", 5, DCH_poz.DCH_month, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"mon", 3, DCH_poz.DCH_mon, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"ms", 2, DCH_poz.DCH_MS, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"p.m.", 4, DCH_poz.DCH_p_m, false, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* p */
      {"pm", 2, DCH_poz.DCH_pm, false, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"q", 1, DCH_poz.DCH_Q, true, FromCharDateMode.FROM_CHAR_DATE_NONE}, /* q */
      {"rm", 2, DCH_poz.DCH_rm, false, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN}, /* r */
      {"ssss", 4, DCH_poz.DCH_SSSS, true, FromCharDateMode.FROM_CHAR_DATE_NONE},	/* s */
      {"ss", 2, DCH_poz.DCH_SS, true, FromCharDateMode.FROM_CHAR_DATE_NONE},
      {"tz", 2, DCH_poz.DCH_tz, false, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* t */
      {"us", 2, DCH_poz.DCH_US, true, FromCharDateMode.FROM_CHAR_DATE_NONE},		/* u */
      {"ww", 2, DCH_poz.DCH_WW, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},	/* w */
      {"w", 1, DCH_poz.DCH_W, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"y,yyy", 5, DCH_poz.DCH_Y_YYY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},	/* y */
      {"yyyy", 4, DCH_poz.DCH_YYYY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"yyy", 3, DCH_poz.DCH_YYY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"yy", 2, DCH_poz.DCH_YY, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN},
      {"y", 1, DCH_poz.DCH_Y, true, FromCharDateMode.FROM_CHAR_DATE_GREGORIAN}
  };

  static final KeyWord[] DCH_keywords = new KeyWord[DCH_keywordValues.length];

  static Map<Character, Integer> DCH_index = new HashMap<Character, Integer>();

  static {
    int index = 0;
    for(Object[] eachKeywordValue: DCH_keywordValues) {
      KeyWord keyword = new KeyWord();
      keyword.name = (String)eachKeywordValue[0];
      keyword.len = ((Integer)eachKeywordValue[1]).intValue();
      keyword.id = ((DCH_poz)eachKeywordValue[2]).getValue();
      keyword.idType = ((DCH_poz)eachKeywordValue[2]);
      keyword.is_digit = ((Boolean)eachKeywordValue[3]).booleanValue();
      keyword.date_mode = (FromCharDateMode)eachKeywordValue[4];

      Character c = new Character(keyword.name.charAt(0));
      Integer pos = DCH_index.get(c);
      if (pos == null) {
        DCH_index.put(c, index);
      }
      DCH_keywords[index++] = keyword;
    }
  }

  /**
   * ----------
   * Format parser structs
   * ----------
   */
  static class KeySuffix {
    String name;			/* suffix string		*/
    int len;			    /* suffix length		*/
    int id;				    /* used in node->suffix */
    int type;			    /* prefix / postfix			*/

    public KeySuffix(String name, int len, int id, int type) {
      this.name = name;
      this.len = len;
      this.id = id;
      this.type = type;
    }
  }

  static class KeyWord {
    String name;
    int len;
    int id;
    DCH_poz idType;
    boolean is_digit;
    FromCharDateMode date_mode;
  }

  static class FormatNode {
    int type;			  /* node type			*/
    KeyWord key;		/* if node type is KEYWORD	*/
    char character;	/* if node type is CHAR		*/
    int suffix;			/* keyword suffix		*/
  }

  static class TmFromChar {
    FromCharDateMode mode = FromCharDateMode.FROM_CHAR_DATE_NONE;
    int	hh;
    int pm;
    int mi;
    int ss;
    int ssss;
    int d;				/* stored as 1-7, Sunday = 1, 0 means missing */
    int dd;
    int ddd;
    int mm;
    int ms;
    int year;
    int bc;
    int ww;
    int w;
    int cc;
    int j;
    int us;
    int yysz;			/* is it YY or YYYY ? */
    int clock;		/* 12 or 24 hour clock? */
  }
  static Map<String, FormatNode[]> formatNodeCache = new HashMap<String, FormatNode[]>();

 /**
  * ----------
  * Skip TM / th in FROM_CHAR
  * ----------
  */
  static int SKIP_THth(int suf)	{
    return (S_THth(suf) != 0 ? 2 : 0);
  }

  /**
   * ----------
   * Suffix tests
   * ----------
   */
  static int S_THth(int s) {
    return ((((s) & DCH_S_TH) != 0 || ((s) & DCH_S_th) != 0) ? 1 : 0);
  }
  static int S_TH(int s) {
    return (((s) & DCH_S_TH) != 0 ? 1 : 0);
  }
  static int S_th(int s) {
    return (((s) & DCH_S_th) != 0 ? 1 : 0);
  }
  static int S_TH_TYPE(int s) {
    return (((s) & DCH_S_TH) != 0 ? TH_UPPER : TH_LOWER);
  }

  static final int TH_UPPER	=	1;
  static final int TH_LOWER = 2;

  /* Oracle toggles FM behavior, we don't; see docs. */
  static int S_FM(int s) {
    return (((s) & DCH_S_FM) != 0 ? 1 : 0);
  }
  static int S_SP(int s) {
    return (((s) & DCH_S_SP) != 0 ? 1 : 0);
  }
  static int S_TM(int s) {
    return (((s) & DCH_S_TM) != 0 ? 1 : 0);
  }

  public static TimeMeta parseDateTime(String dateText, String formatText) {
    TimeMeta tm = new TimeMeta();

    //TODO consider TimeZone
    doToTimestamp(dateText, formatText, tm);

    // when we parse some date without day like '2014-04', we should set day to 1.
    if (tm.dayOfMonth == 0) {
      tm.dayOfMonth = 1;
    }

    if (tm.dayOfYear > 0 && tm.dayOfMonth > 0) {
      tm.dayOfYear = 0;
    }

    return tm;
  }

  /**
   * Make Timestamp from date_str which is formatted at argument 'fmt'
   * ( toTimestamp is reverse to_char() )
   * @param dateText
   * @param formatText
   * @return
   */
  public static TimestampDatum toTimestamp(String dateText, String formatText) {
    TimeMeta tm = parseDateTime(dateText, formatText);

    return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm));
  }

  /**
   * Parse the 'dateText' according to 'formatText', return results as a TimeMeta tm
   * and fractional seconds.
   *
   * We parse 'formatText' into a list of FormatNodes, which is then passed to
   * DCH_from_char to populate a TmFromChar with the parsed contents of
   * 'dateText'.
   *
   * The TmFromChar is then analysed and converted into the final results in struct 'tm'.
   *
   * This function does very little error checking, e.g.
   * to_timestamp('20096040','YYYYMMDD') works
   * @param dateText
   * @param formatText
   * @param tm
   */
  static void doToTimestamp(String dateText, String formatText, TimeMeta tm) {
    TmFromChar tmfc = new TmFromChar();
    int formatLength = formatText.length();

    if (formatLength > 0) {
      FormatNode[] formatNodes;
      synchronized(formatNodeCache) {
        formatNodes = formatNodeCache.get(formatText);
      }

      if (formatNodes == null) {
        formatNodes = new FormatNode[formatLength + 1];
        for (int i = 0; i < formatNodes.length; i++) {
          formatNodes[i] = new FormatNode();
        }
        parseFormat(formatNodes, formatText, FORMAT_TYPE.DCH_TYPE);
        formatNodes[formatLength].type = NODE_TYPE_END;	/* Paranoia? */

        synchronized(formatNodeCache) {
          formatNodeCache.put(formatText, formatNodes);
        }
      }
      DCH_from_char(formatNodes, dateText, tmfc);
    }

    /*
     * Convert values that user define for FROM_CHAR (to_date/to_timestamp) to
     * standard 'tm'
     */
    if (tmfc.ssss != 0) {
      int x = tmfc.ssss;

      tm.hours = x / DateTimeConstants.SECS_PER_HOUR;
      x %= DateTimeConstants.SECS_PER_HOUR;
      tm.minutes = x / DateTimeConstants.SECS_PER_MINUTE;
      x %= DateTimeConstants.SECS_PER_MINUTE;
      tm.secs = x;
    }

    if (tmfc.ss != 0) {
      tm.secs = tmfc.ss;
    }
    if (tmfc.mi != 0) {
      tm.minutes = tmfc.mi;
    }
    if (tmfc.hh != 0) {
      tm.hours = tmfc.hh;
    }

    if (tmfc.clock == CLOCK_12_HOUR) {
      if (tm.hours < 1 || tm.hours > HOURS_PER_DAY / 2) {
        throw new IllegalArgumentException(
            "hour \"" + tm.hours + "\" is invalid for the 12-hour clock, " +
                "Use the 24-hour clock, or give an hour between 1 and 12.");
      }
      if (tmfc.pm != 0 && tm.hours < HOURS_PER_DAY / 2) {
        tm.hours += HOURS_PER_DAY / 2;
      } else if (tmfc.pm == 0 && tm.hours == HOURS_PER_DAY / 2) {
        tm.hours = 0;
      }
    }

    if (tmfc.year != 0) {
      /*
       * If CC and YY (or Y) are provided, use YY as 2 low-order digits for
       * the year in the given century.  Keep in mind that the 21st century
       * AD runs from 2001-2100, not 2000-2099; 6th century BC runs from
       * 600BC to 501BC.
       */
      if (tmfc.cc != 0 && tmfc.yysz <= 2) {
        if (tmfc.bc != 0) {
          tmfc.cc = -tmfc.cc;
        }
        tm.years = tmfc.year % 100;
        if (tm.years != 0) {
          if (tmfc.cc >= 0) {
            tm.years += (tmfc.cc - 1) * 100;
          } else {
            tm.years = (tmfc.cc + 1) * 100 - tm.years + 1;
          }
        } else {
				  /* find century year for dates ending in "00" */
          tm.years = tmfc.cc * 100 + ((tmfc.cc >= 0) ? 0 : 1);
        }
      } else {
			  /* If a 4-digit year is provided, we use that and ignore CC. */
        tm.years = tmfc.year;
        if (tmfc.bc != 0 && tm.years > 0) {
          tm.years = -(tm.years - 1);
        }
      }
    }
    else if (tmfc.cc != 0) {			/* use first year of century */
      if (tmfc.bc != 0) {
        tmfc.cc = -tmfc.cc;
      }
      if (tmfc.cc >= 0) {
			  /* +1 becuase 21st century started in 2001 */
        tm.years = (tmfc.cc - 1) * 100 + 1;
      } else {
			  /* +1 because year == 599 is 600 BC */
        tm.years = tmfc.cc * 100 + 1;
      }
    }

    if (tmfc.j != 0) {
      DateTimeUtil.j2date(tmfc.j, tm);
    }
    if (tmfc.ww != 0) {
      if (tmfc.mode == FromCharDateMode.FROM_CHAR_DATE_ISOWEEK) {
        /*
         * If tmfc.d is not set, then the date is left at the beginning of
         * the ISO week (Monday).
         */
        if (tmfc.d != 0) {
          DateTimeUtil.isoweekdate2date(tmfc.ww, tmfc.d, tm);
        } else {
          DateTimeUtil.isoweek2date(tmfc.ww, tm);
        }
      } else {
        tmfc.ddd = (tmfc.ww - 1) * 7 + 1;
      }
    }

    if (tmfc.w != 0) {
      tmfc.dd = (tmfc.w - 1) * 7 + 1;
    }
    if (tmfc.d != 0) {
      //tm.tm_wday = tmfc.d - 1;		/* convert to native numbering */
    }
    if (tmfc.dd != 0) {
      tm.dayOfMonth = tmfc.dd;
    }
    if (tmfc.ddd != 0) {
      tm.dayOfYear = tmfc.ddd;
    }
    if (tmfc.mm != 0) {
      tm.monthOfYear = tmfc.mm;
    }
    if (tmfc.ddd != 0 && (tm.monthOfYear <= 1 || tm.dayOfMonth <= 1)) {
      /*
       * The month and day field have not been set, so we use the
       * day-of-year field to populate them.	Depending on the date mode,
       * this field may be interpreted as a Gregorian day-of-year, or an ISO
       * week date day-of-year.
       */
      if (tm.years == 0 && tmfc.bc == 0) {
        throw new IllegalArgumentException("cannot calculate day of year without year information");
      }
      if (tmfc.mode == FromCharDateMode.FROM_CHAR_DATE_ISOWEEK) {
        /* zeroth day of the ISO year, in Julian */
        int j0 = DateTimeUtil.isoweek2j(tm.years, 1) - 1;
        DateTimeUtil.j2date(j0 + tmfc.ddd, tm);
      } else {
        int	i;

        boolean leap = DateTimeUtil.isLeapYear(tm.years);
        int[] y = ysum[leap ? 1 : 0];

        for (i = 1; i <= MONTHS_PER_YEAR; i++) {
          if (tmfc.ddd < y[i])
            break;
        }
        if (tm.monthOfYear <= 1) {
          tm.monthOfYear = i;
        }

        if (tm.dayOfMonth <= 1) {
          tm.dayOfMonth = tmfc.ddd - y[i - 1];
        }
        tm.dayOfYear = 0;
      }
    }

    if (tmfc.ms != 0) {
      tm.fsecs += tmfc.ms * 1000;
    }
    if (tmfc.us != 0) {
      tm.fsecs += tmfc.us;
    }
  }

  /**
   * Format parser, search small keywords and keyword's suffixes, and make
   * format-node tree.
   *
   * for DATE-TIME & NUMBER version
   * @param node
   * @param str
   * @param ver
   */
  static void parseFormat(FormatNode[] node, String str, FORMAT_TYPE ver) {
    KeySuffix  s;
    boolean	node_set = false;
    int suffix;
    int last = 0;

    int nodeIndex = 0;
    int charIdx = 0;
    char[] chars = str.toCharArray();

    while (charIdx < chars.length) {
      suffix = 0;

      // Prefix
      if (ver == FORMAT_TYPE.DCH_TYPE && (s = suff_search(chars, charIdx, SUFFTYPE_PREFIX)) != null) {
        suffix |= s.id;
        if (s.len > 0) {
          charIdx += s.len;
        }
      }

      // Keyword
      if (charIdx < chars.length && (node[nodeIndex].key = index_seq_search(chars, charIdx)) != null) {
        node[nodeIndex].type = NODE_TYPE_ACTION;
        node[nodeIndex].suffix = 0;
        node_set = true;
        if (node[nodeIndex].key.len > 0) {
          charIdx += node[nodeIndex].key.len;
        }

        // NUM version: Prepare global NUMDesc struct
        if (ver == FORMAT_TYPE.NUM_TYPE) {
          //NUMDesc_prepare(Num, node);
        }

         // Postfix
        if (ver == FORMAT_TYPE.DCH_TYPE && charIdx < chars.length  && (s = suff_search(chars, charIdx, SUFFTYPE_POSTFIX)) != null) {
          suffix |= s.id;
          if (s.len > 0) {
            charIdx += s.len;
          }
        }
      } else if (charIdx < chars.length) {
        // Special characters '\' and '"'
        if (chars[charIdx] == '"' && last != '\\') {
          int			x = 0;

          while (charIdx < chars.length ) {
            charIdx++;
            if (chars[charIdx] == '"' && x != '\\') {
              charIdx++;
              break;
            } else if (chars[charIdx] == '\\' && x != '\\') {
              x = '\\';
              continue;
            }
            node[nodeIndex].type = NODE_TYPE_CHAR;
            node[nodeIndex].character = chars[charIdx];
            node[nodeIndex].key = null;
            node[nodeIndex].suffix = 0;
            nodeIndex++;
            x = chars[charIdx];
          }
          node_set = false;
          suffix = 0;
          last = 0;
        } else if (charIdx < chars.length - 1 && chars[charIdx] == '\\' && last != '\\' && chars[charIdx + 1] == '"') {
          last = chars[charIdx];
          charIdx++;
        } else if (charIdx < chars.length) {
          node[nodeIndex].type = NODE_TYPE_CHAR;
          node[nodeIndex].character = chars[charIdx];
          node[nodeIndex].key = null;
          node_set = true;
          last = 0;
          charIdx++;
        }
      }

		  // end
      if (node_set) {
        if (node[nodeIndex].type == NODE_TYPE_ACTION) {
          node[nodeIndex].suffix = suffix;
        }
        nodeIndex++;
        node[nodeIndex].suffix = 0;
        node_set = false;
      }
    }

    node[nodeIndex].type = NODE_TYPE_END;
    node[nodeIndex].suffix = 0;
  }

  /**
   * Process a string as denoted by a list of FormatNodes.
   * The TmFromChar struct pointed to by 'out' is populated with the results.
   *
   * Note: we currently don't have any to_interval() function, so there
   * is no need here for INVALID_FOR_INTERVAL checks.
   * @param nodes
   * @param dateText
   * @param out
   */
  static void DCH_from_char(FormatNode[] nodes, String dateText, TmFromChar out) {
    int	len;
    AtomicInteger value = new AtomicInteger();
    boolean	fx_mode = false;

    char[] chars = dateText.toCharArray();
    int charIdx = 0;
    int nodeIdx = 0;
    for (; nodeIdx < nodes.length; nodeIdx++) {
      FormatNode node = nodes[nodeIdx];
      if (node.type == NODE_TYPE_END || charIdx >= chars.length) {
        break;
      }
      if (node.type != NODE_TYPE_ACTION) {
        charIdx++;
			  /* Ignore spaces when not in FX (fixed width) mode */
        if (Character.isSpaceChar(node.character) && !fx_mode) {
          while (charIdx < chars.length && Character.isSpaceChar(chars[charIdx])) {
            charIdx++;
          }
        }
        continue;
      }

      from_char_set_mode(out, node.key.date_mode);

      switch (node.key.idType) {
        case DCH_FX:
          fx_mode = true;
          break;
        case DCH_A_M:
        case DCH_P_M:
        case DCH_a_m:
        case DCH_p_m:
          value.set(out.pm);
          charIdx += from_char_seq_search(value, dateText, charIdx, ampm_strings_long, ALL_UPPER, node.key.len, node);
          assertOutValue(out.pm, value.get() % 2, node);
          out.pm = value.get() % 2;
          out.clock = CLOCK_12_HOUR;
          break;
        case DCH_AM:
        case DCH_PM:
        case DCH_am:
        case DCH_pm:
          value.set(out.pm);
          charIdx += from_char_seq_search(value, dateText, charIdx, ampm_strings, ALL_UPPER, node.key.len, node);
          assertOutValue(out.pm, value.get() % 2, node);
          out.pm = value.get() % 2;
          out.clock = CLOCK_12_HOUR;
          break;
        case DCH_HH:
        case DCH_HH12:
          value.set(out.hh);
          charIdx += from_char_parse_int_len(value, dateText, charIdx, 2, nodes, nodeIdx);
          out.hh = value.get();
          out.clock = CLOCK_12_HOUR;
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_HH24:
          value.set(out.hh);
          charIdx += from_char_parse_int_len(value, dateText, charIdx, 2, nodes, nodeIdx);
          out.hh = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_MI:
          value.set(out.mi);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.mi = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_SS:
          value.set(out.ss);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.ss = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_MS:		/* millisecond */
          value.set(out.ms);
          len = from_char_parse_int_len(value, dateText, charIdx, 3, nodes, nodeIdx);
          charIdx += len;
          out.ms = value.get();
          /*
           * 25 is 0.25 and 250 is 0.25 too; 025 is 0.025 and not 0.25
           */
          out.ms *= len == 1 ? 100 :
              len == 2 ? 10 : 1;

          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_US:		/* microsecond */
          value.set(out.us);
          len = from_char_parse_int_len(value, dateText, charIdx, 6, nodes, nodeIdx);
          charIdx += len;
          out.us = value.get();
          out.us *= len == 1 ? 100000 :
              len == 2 ? 10000 :
                  len == 3 ? 1000 :
                      len == 4 ? 100 :
                          len == 5 ? 10 : 1;

          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_SSSS:
          value.set(out.ssss);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.ssss = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_tz:
        case DCH_TZ:
          throw new IllegalArgumentException("\"TZ\"/\"tz\" format patterns are not supported in to_date");
        case DCH_A_D:
        case DCH_B_C:
        case DCH_a_d:
        case DCH_b_c:
          value.set(out.bc);
          charIdx += from_char_seq_search(value, dateText, charIdx, adbc_strings_long, ALL_UPPER, node.key.len, node);
          assertOutValue(out.bc, value.get() % 2, node);
          out.bc = value.get() % 2;
          break;
        case DCH_AD:
        case DCH_BC:
        case DCH_ad:
        case DCH_bc:
          value.set(out.bc);
          charIdx += from_char_seq_search(value, dateText, charIdx, adbc_strings, ALL_UPPER, node.key.len, node);
          assertOutValue(out.bc, value.get() % 2, node);
          out.bc = value.get() % 2;
          break;
        case DCH_MONTH:
        case DCH_Month:
        case DCH_month:
          value.set(out.mm);
          charIdx += from_char_seq_search(value, dateText, charIdx, months_full, ONE_UPPER, MAX_MONTH_LEN, node);
          assertOutValue(out.mm, value.get() + 1, node);
          out.mm = value.get() + 1;
          break;
        case DCH_MON:
        case DCH_Mon:
        case DCH_mon:
          value.set(out.mm);
          charIdx += from_char_seq_search(value, dateText, charIdx, months_short, ONE_UPPER, MAX_MON_LEN, node);
          assertOutValue(out.mm, value.get() + 1, node);
          out.mm = value.get() + 1;
          break;
        case DCH_MM:
          value.set(out.mm);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.mm = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_DAY:
        case DCH_Day:
        case DCH_day:
          value.set(out.d);
          charIdx += from_char_seq_search(value, dateText, charIdx, days_full, ONE_UPPER, MAX_DAY_LEN, node);
          assertOutValue(out.d, value.get(), node);
          out.d = value.get();
          out.d++;
          break;
        case DCH_DY:
        case DCH_Dy:
        case DCH_dy:
          value.set(out.d);
          charIdx += from_char_seq_search(value, dateText, charIdx, days_full, ONE_UPPER, MAX_DY_LEN, node);
          assertOutValue(out.d, value.get(), node);
          out.d = value.get();
          out.d++;
          break;
        case DCH_DDD:
          value.set(out.ddd);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.ddd = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_IDDD:
          value.set(out.ddd);
          charIdx += from_char_parse_int_len(value, dateText, charIdx, 3, nodes, nodeIdx);
          out.ddd = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_DD:
          value.set(out.dd);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.dd = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_D:
          value.set(out.d);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.d = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_ID:
          value.set(out.d);
          charIdx += from_char_parse_int_len(value, dateText, charIdx, 1, nodes, nodeIdx);
          out.d = value.get();
				  /* Shift numbering to match Gregorian where Sunday = 1 */
          if (++out.d > 7) {
            out.d = 1;
          }
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_WW:
        case DCH_IW:
          value.set(out.ww);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.ww = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_Q:
          /*
           * We ignore 'Q' when converting to date because it is unclear
           * which date in the quarter to use, and some people specify
           * both quarter and month, so if it was honored it might
           * conflict with the supplied month. That is also why we don't
           * throw an error.
           *
           * We still parse the source string for an integer, but it
           * isn't stored anywhere in 'out'.
           */
          charIdx += from_char_parse_int(null, dateText, charIdx, nodes, nodeIdx);
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_CC:
          value.set(out.cc);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.cc = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_Y_YYY: {
            int commaIndex = dateText.indexOf(",", charIdx);
            if (commaIndex <= 0) {
              throw new IllegalArgumentException("invalid input string for \"Y,YYY\"");
            }
            int millenia = Integer.parseInt(dateText.substring(charIdx, commaIndex));
            int years = Integer.parseInt(dateText.substring(commaIndex + 1, commaIndex + 1 + 3));
            years += (millenia * 1000);
            assertOutValue(out.year, years, node);
            out.year = years;
            out.yysz = 4;
            charIdx += strdigits_len(dateText, charIdx) + 4 + SKIP_THth(node.suffix);
          }
          break;
        case DCH_YYYY:
        case DCH_IYYY:
          value.set(out.year);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.year = value.get();
          out.yysz = 4;
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_YYY:
        case DCH_IYY: {
            int retVal = from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
            charIdx += retVal;
            out.year = value.get();
            if (retVal < 4) {
              out.year = adjust_partial_year_to_2020(out.year);
            }
            out.yysz = 3;
            charIdx += SKIP_THth(node.suffix);
            }
          break;
        case DCH_YY:
        case DCH_IY: {
            value.set(out.year);
            int retVal = from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
            charIdx += retVal;
            out.year = value.get();
            if (retVal < 4) {
              out.year = adjust_partial_year_to_2020(out.year);
            }
            out.yysz = 2;
            charIdx += SKIP_THth(node.suffix);
          }
          break;
        case DCH_Y:
        case DCH_I:
          value.set(out.year);
          int retVal = from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          charIdx += retVal;
          out.year = value.get();
          if (retVal < 4) {
            out.year = adjust_partial_year_to_2020(out.year);
          }
          out.yysz = 1;
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_RM:
          value.set(out.mm);
          charIdx += from_char_seq_search(value, dateText, charIdx, rm_months_upper, ALL_UPPER, MAX_RM_LEN, node);
          assertOutValue(out.mm, MONTHS_PER_YEAR - value.get(), node);
          out.mm = MONTHS_PER_YEAR - value.get();
          break;
        case DCH_rm:
          value.set(out.mm);
          charIdx += from_char_seq_search(value, dateText, charIdx, rm_months_lower, ALL_LOWER, MAX_RM_LEN, node);
          assertOutValue(out.mm, MONTHS_PER_YEAR - value.get(), node);
          out.mm = MONTHS_PER_YEAR - value.get();
          break;
        case DCH_W:
          value.set(out.w);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.w = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
        case DCH_J:
          value.set(out.j);
          charIdx += from_char_parse_int(value, dateText, charIdx, nodes, nodeIdx);
          out.j = value.get();
          charIdx += SKIP_THth(node.suffix);
          break;
      }
    }
  }

  static KeySuffix suff_search(char[] chars, int startIdx, int type) {
    for (KeySuffix eachSuffix: DCH_suff) {
      if (eachSuffix.type != type) {
        continue;
      }

      if (strncmp(chars, startIdx, eachSuffix.name, eachSuffix.len)) {
        return eachSuffix;
      }
    }
    return null;
  }

  /**
   * Fast sequential search, use index for data selection which
   * go to seq. cycle (it is very fast for unwanted strings)
   * (can't be used binary search in format parsing)
   * @param chars
   * @param startIdx
   * @return
   */
  static KeyWord index_seq_search(char[] chars, int startIdx) {
    if (KeyWord_INDEX_FILTER(chars[startIdx]) == 0) {
      return null;
    }

    Integer pos = DCH_index.get(chars[startIdx]);

    if (pos != null) {
      KeyWord keyword = DCH_keywords[pos];
      do {
        if (strncmp(chars, startIdx, keyword.name, keyword.len)) {
          return keyword;
        }
        pos++;
        if (pos >=  DCH_keywords.length) {
          return null;
        }
        keyword = DCH_keywords[pos];
      } while (chars[startIdx] == keyword.name.charAt(0));
    }
    return null;
  }

  static boolean strncmp(char[] chars, int startIdx, String str, int len) {
    if (chars.length - startIdx < len) {
      return false;
    }

    int index = startIdx;
    for (int i = 0; i < len; i++, index++) {
      if (chars[index] != str.charAt(i)) {
        return false;
      }
    }

    return true;
  }

  static int KeyWord_INDEX_FILTER(char c)	{
    return ((c) <= ' ' || (c) >= '~' ? 0 : 1);
  }

  /**
   * Set the date mode of a from-char conversion.
   *
   * Puke if the date mode has already been set, and the caller attempts to set
   * it to a conflicting mode.
   * @param tmfc
   * @param mode
   */
  static void from_char_set_mode(TmFromChar tmfc, FromCharDateMode mode) {
    if (mode != FromCharDateMode.FROM_CHAR_DATE_NONE) {
      if (tmfc.mode == FromCharDateMode.FROM_CHAR_DATE_NONE) {
        tmfc.mode = mode;
      } else if (tmfc.mode != mode) {
        throw new IllegalArgumentException("invalid combination of date conventions: " +
                "Do not mix Gregorian and ISO week date " +
                "conventions in a formatting template.");
      }
    }
  }

  /**
   * Perform a sequential search in 'array' for text matching the first 'max'
   * characters of the source string.
   *
   * If a match is found, copy the array index of the match into the integer
   * pointed to by 'dest', advance 'src' to the end of the part of the string
   * which matched, and return the number of characters consumed.
   *
   * If the string doesn't match, throw an error.
   * @param dest
   * @param src
   * @param charIdx
   * @param array
   * @param type
   * @param max
   * @param node
   * @return
   */
  static int from_char_seq_search(AtomicInteger dest, String src, int charIdx, String[] array, int type, int max,
                       FormatNode node) {
    AtomicInteger len = new AtomicInteger(0);

    dest.set(seq_search(src, charIdx, array, type, max, len));
    if (len.get() <= 0) {
      String copy;
      if (charIdx + node.key.len >= src.length()) {
        copy = src.substring(charIdx, charIdx + node.key.len);
      } else {
        copy = src.substring(charIdx);
      }
      throw new IllegalArgumentException("Invalid value \"" + copy + "\" for \"" + node.key.name + "\". " +
          "The given value did not match any of the allowed values for this field.");
    }
    return len.get();
  }

  /**
   * Sequential search with to upper/lower conversion
   * @param name
   * @param charIdx
   * @param array
   * @param type
   * @param max
   * @param len
   * @return
   */
  static int seq_search(String name, int charIdx, String[] array, int type, int max, AtomicInteger len) {
    if (name == null || name.length() <= charIdx) {
      return -1;
    }

    char[] nameChars = name.toCharArray();
    char nameChar = nameChars[charIdx];

	  /* set first char */
    if (type == ONE_UPPER || type == ALL_UPPER) {
      nameChar = Character.toUpperCase(nameChar);
    } else if (type == ALL_LOWER) {
      nameChar = Character.toLowerCase(nameChar);
    }

    int arrayIndex = 0;
    for (int last = 0; array[arrayIndex] != null; arrayIndex++) {
      String arrayStr = array[arrayIndex];
		  /* comperate first chars */
      if (nameChar != arrayStr.charAt(0)) {
        continue;
      }
      int arrayStrLen = arrayStr.length();
      int arrayCharIdx = 1;
      int nameCharIdx = charIdx + 1;

      for (int idx = 1; ; nameCharIdx++, arrayCharIdx++, idx++) {
			  // search fragment (max) only
        if (max != 0 && idx == max) {
          len.set(idx + 1);   // '\0'
          return arrayIndex;
        }
			  // full size
        if (arrayCharIdx == arrayStrLen - 1) {
          len.set(idx + 1);   // '\0'
          return arrayIndex;
        }
        // Not found in array 'a'
        if (nameCharIdx == nameChars.length - 1) {
          break;
        }
        /*
         * Convert (but convert new chars only)
         */
        nameChar = nameChars[nameCharIdx];
        if (idx > last) {
          if (type == ONE_UPPER || type == ALL_LOWER) {
            nameChar = Character.toLowerCase(nameChar);
          } else if (type == ALL_UPPER) {
            nameChar = Character.toUpperCase(nameChar);
          }
          last = idx;
        }
        if (nameChar != arrayStr.charAt(arrayCharIdx)){
          break;
        }
      }
    }

    return -1;
  }

  /**
   * Read a single integer from the source string, into the int pointed to by
   * 'dest'. If 'dest' is NULL, the result is discarded.
   *
   * In fixed-width mode (the node does not have the FM suffix), consume at most
   * 'len' characters.  However, any leading whitespace isn't counted in 'len'.
   *
   * We use strtol() to recover the integer value from the source string, in
   * accordance with the given FormatNode.
   *
   * If the conversion completes successfully, src will have been advanced to
   * point at the character immediately following the last character used in the
   * conversion.
   *
   * Return the number of characters consumed.
   *
   * Note that from_char_parse_int() provides a more convenient wrapper where
   * the length of the field is the same as the length of the format keyword (as
   * with DD and MI).
   * @param dest
   * @param src
   * @param charIdx
   * @param len
   * @param nodes
   * @param nodeIndex
   * @return
   */
  static int from_char_parse_int_len(AtomicInteger dest, String src, int charIdx, int len, FormatNode[] nodes, int nodeIndex) {
    long result;
    int	 initCharIdx = charIdx;
    StringBuilder tempSb = new StringBuilder();

    /*
     * Skip any whitespace before parsing the integer.
     */
    charIdx = strspace_len(src, charIdx);

    int used = src.length() <= charIdx + len ? src.length() - (charIdx + len) : len;
    if (used <= 0) {
      used = src.length() - charIdx;
    }
    String copy = src.substring(charIdx, charIdx + used);

    if (S_FM(nodes[nodeIndex].suffix) != 0 || is_next_separator(nodes, nodeIndex)) {
		/*
		 * This node is in Fill Mode, or the next node is known to be a
		 * non-digit value, so we just slurp as many characters as we can get.
		 */
      result = DateTimeUtil.strtol(src, charIdx, tempSb);
      charIdx = src.length() - tempSb.length();
    } else {

      /*
       * We need to pull exactly the number of characters given in 'len' out
       * of the string, and convert those.
       */
      if (used < len) {
        throw new IllegalArgumentException("source string too short for \"" + nodes[nodeIndex].key.name + "\" + formatting field");
      }
      result = DateTimeUtil.strtol(copy, 0, tempSb);
      used = copy.length() - tempSb.length();

      if (used > 0 && used < len) {
        throw new IllegalArgumentException("invalid value \"" + copy + "\" for \"" + nodes[nodeIndex].key.name + "\"." +
            "Field requires " + len + " characters, but only " + used);
      }
      charIdx += used;
    }

    if (charIdx == initCharIdx) {
      throw new IllegalArgumentException("invalid value \"" + copy + "\" for \"" + nodes[nodeIndex].key.name + "\"." +
          "Value must be an integer.");
    }
    if (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("value for \"" + nodes[nodeIndex].key.name + "\"" +
          " in source string is out of range." +
          "Value must be in the range " + Integer.MIN_VALUE + " to " + Integer.MAX_VALUE + ".");
    }
    if (dest != null) {
      assertOutValue(dest.get(), (int)result, nodes[nodeIndex]);
      dest.set((int)result);
    }
    return charIdx - initCharIdx;
  }

  /**
   * Call from_char_parse_int_len(), using the length of the format keyword as
   * the expected length of the field.
   *
   * Don't call this function if the field differs in length from the format
   * keyword (as with HH24; the keyword length is 4, but the field length is 2).
   * In such cases, call from_char_parse_int_len() instead to specify the
   * required length explicitly.
   * @param dest
   * @param src
   * @param charIdx
   * @param nodes
   * @param nodeIdx
   * @return
   */
  static int from_char_parse_int(AtomicInteger dest, String src, int charIdx, FormatNode[] nodes, int nodeIdx) {
    return from_char_parse_int_len(dest, src, charIdx, nodes[nodeIdx].key.len, nodes, nodeIdx);
  }

  static int strspace_len(String str, int charIdx) {
    int len = str.length();
    while (charIdx < len && Character.isSpaceChar(str.charAt(charIdx))) {
      charIdx++;
    }
    return charIdx;
  }

  static int strdigits_len(String str, int charIdx) {
    int len = strspace_len(str, charIdx);
    int index = charIdx + len;

    int strLen = str.length();

    while (index < strLen && Character.isDigit(str.charAt(index)) && len <= DCH_MAX_ITEM_SIZ) {
      len++;
      index++;
    }
    return len;
  }

  static void assertOutValue(int dest, int value, FormatNode node) {
    if (dest != 0 && dest != value) {
      throw new IllegalArgumentException(
          "conflicting values for \"" + node.key.name + "\" field in formatting string," +
          "This value contradicts a previous setting for the same field type(" + dest + "," + value + ")");
    }
  }

  /**
   * Return true if next format picture is not digit value
   * @param nodes
   * @param nodeIndex
   * @return
   */
  static boolean is_next_separator(FormatNode[] nodes, int nodeIndex) {
    int index = nodeIndex;
    if (nodes[index].type == NODE_TYPE_END) {
      return false;
    }

    if (nodes[index].type == NODE_TYPE_ACTION && S_THth(nodes[index].suffix) != 0) {
      return true;
    }

    /*
     * Next node
     */
    index++;

	/* end of format string is treated like a non-digit separator */
    if (nodes[index].type == NODE_TYPE_END) {
      return true;
    }

    if (nodes[index].type == NODE_TYPE_ACTION) {
      return nodes[index].key.is_digit ? false : true;
    } else if (Character.isDigit(nodes[index].character)) {
      return false;
    }

    return true;				/* some non-digit input (separator) */
  }

  /**
   * Adjust all dates toward 2020; this is effectively what happens when we
   * assume '70' is 1970 and '69' is 2069.
   * @param year
   * @return
   */
  static int adjust_partial_year_to_2020(int year) {
    if (year < 70) {
      /* Force 0-69 into the 2000's */
      return year + 2000;
    } else if (year < 100) {
      /* Force 70-99 into the 1900's */
      return year + 1900;
    } else if (year < 520) {
      /* Force 100-519 into the 2000's */
      return year + 2000;
    } else if (year < 1000) {
      /* Force 520-999 into the 1000's */
      return year + 1000;
    } else {
      return year;
    }
  }

  /**
   * Converts TimeMeta to a string using given the format pattern text.
   * @param tm
   * @param formatText
   * @return
   */
  public static String to_char(TimeMeta tm, String formatText) {
    int fmt_len = formatText.length();

    StringBuilder out = new StringBuilder();
    if (fmt_len > 0) {
      FormatNode[] formatNodes;

      synchronized(formatNodeCache) {
        formatNodes = formatNodeCache.get(formatText);
      }

      if (formatNodes == null) {
        formatNodes = new FormatNode[fmt_len + 1];
        for (int i = 0; i < formatNodes.length; i++) {
          formatNodes[i] = new FormatNode();
        }
        parseFormat(formatNodes, formatText, FORMAT_TYPE.DCH_TYPE);
        formatNodes[fmt_len].type = NODE_TYPE_END;	/* Paranoia? */

        synchronized(formatNodeCache) {
          formatNodeCache.put(formatText, formatNodes);
        }
      }
      DCH_to_char(formatNodes, false, tm, out);
      return out.toString();
    } else {
      throw new IllegalArgumentException("No format text.");
    }
  }

  /**
   * Process a TmToChar struct as denoted by a list of FormatNodes.
   * The formatted data is written to the string pointed to by 'out'.
   * @param nodes
   * @param isInterval
   * @param tm
   * @param out
   */
  private static void DCH_to_char(FormatNode[] nodes, boolean isInterval, TimeMeta tm, StringBuilder out) {
    int i;
    for (FormatNode node: nodes) {
      if (node.type == NODE_TYPE_END) {
        break;
      }
      if (node.type != NODE_TYPE_ACTION) {
        out.append(node.character);
        continue;
      }

      switch (node.key.idType) {
        case DCH_A_M:
        case DCH_P_M:
          out.append((tm.hours % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? P_M_STR : A_M_STR);
          break;
        case DCH_AM:
        case DCH_PM:
          out.append((tm.hours % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? PM_STR : AM_STR);
          break;
        case DCH_a_m:
        case DCH_p_m:
          out.append((tm.hours % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? p_m_STR : a_m_STR);
          break;
        case DCH_am:
        case DCH_pm:
          out.append((tm.hours % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? pm_STR : am_STR);
          break;
        case DCH_HH:
        case DCH_HH12: {
          /*
           * display time as shown on a 12-hour clock, even for
           * intervals
           */
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%02d");
          out.append(String.format(formatStr,
              tm.hours % (HOURS_PER_DAY / 2) == 0 ? HOURS_PER_DAY / 2 : tm.hours % (HOURS_PER_DAY / 2)));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_HH24: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%02d");
          out.append(String.format(formatStr, tm.hours));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_MI: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%02d");
          out.append(String.format(formatStr, tm.minutes));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_SS: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%02d");
          out.append(String.format(formatStr, tm.secs));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_MS:		/* millisecond */
          out.append(String.format("%03d", (int) (tm.fsecs / 1000.0)));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_US:		/* microsecond */
          out.append(String.format("%06d", (int) tm.fsecs));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_SSSS:
          out.append(String.format("%d", tm.hours * DateTimeConstants.SECS_PER_HOUR +
              tm.minutes * DateTimeConstants.SECS_PER_MINUTE + tm.secs));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_tz:
          invalidForInterval(isInterval, node);
          //TODO
          break;
        case DCH_A_D:
        case DCH_B_C:
          invalidForInterval(isInterval, node);
          out.append((tm.years <= 0 ? B_C_STR : A_D_STR));
          break;
        case DCH_AD:
        case DCH_BC:
          invalidForInterval(isInterval, node);
          out.append((tm.years <= 0 ? BC_STR : AD_STR));
          break;
        case DCH_a_d:
        case DCH_b_c:
          invalidForInterval(isInterval, node);
          out.append((tm.years <= 0 ? b_c_STR : a_d_STR));
          break;
        case DCH_ad:
        case DCH_bc:
          invalidForInterval(isInterval, node);
          out.append((tm.years <= 0 ? bc_STR : ad_STR));
          break;
        case DCH_MONTH:
          invalidForInterval(isInterval, node);
          if (tm.monthOfYear == 0) {
            break;
          }
          if (S_TM(node.suffix) != 0) {
            out.append(months_full[tm.monthOfYear - 1].toUpperCase());
          } else {
            String formatStr =(S_FM(node.suffix) != 0 ? "%0d": "%-09d");
            out.append(String.format(formatStr, months_full[tm.monthOfYear - 1].toUpperCase()));
          }
          break;
        case DCH_Month:
          invalidForInterval(isInterval, node);
          if (tm.monthOfYear == 0) {
            break;
          }
          if (S_TM(node.suffix) != 0) {
            out.append(months_full[tm.monthOfYear - 1]);
          } else {
            String formatStr = (S_FM(node.suffix) != 0 ? "%s": "%-9s");
            out.append(String.format(formatStr, months_full[tm.monthOfYear - 1]));
          }
          break;
        case DCH_month:
          invalidForInterval(isInterval, node);
          if (tm.monthOfYear == 0) {
            break;
          }
          if (S_TM(node.suffix) != 0) {
            out.append(months_full[tm.monthOfYear - 1].toLowerCase());
          } else {
            String formatStr = (S_FM(node.suffix) != 0 ? "%s": "%-9s");
            out.append(String.format(formatStr, months_full[tm.monthOfYear - 1].toLowerCase()));
          }
          break;
        case DCH_MON:
          invalidForInterval(isInterval, node);
          if (tm.monthOfYear == 0) {
            break;
          }
          if (S_TM(node.suffix) != 0) {
            out.append(months_short[tm.monthOfYear - 1].toUpperCase());
          } else {
            out.append(months_short[tm.monthOfYear - 1].toUpperCase());
          }
          break;
        case DCH_Mon:
          invalidForInterval(isInterval, node);
          if (tm.monthOfYear == 0) {
            break;
          }
          if (S_TM(node.suffix) != 0) {
            out.append(months_short[tm.monthOfYear - 1]);
          } else {
            out.append(months_short[tm.monthOfYear - 1]);
          }
          break;
        case DCH_mon:
          invalidForInterval(isInterval, node);
          if (tm.monthOfYear == 0)
            break;
          if (S_TM(node.suffix) != 0) {
            out.append(months_short[tm.monthOfYear - 1].toLowerCase());
          } else {
            out.append(months_short[tm.monthOfYear - 1].toLowerCase());
          }
          break;
        case DCH_MM: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%02d");
          out.append(String.format(formatStr, tm.monthOfYear));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_DAY: {
          invalidForInterval(isInterval, node);
          if (S_TM(node.suffix) != 0) {
            out.append(days_full[tm.getDayOfWeek()].toUpperCase());
          } else {
            String formatStr = (S_FM(node.suffix) != 0 ? "%s" : "%-9s");
            out.append(String.format(formatStr, days_full[tm.getDayOfWeek()].toUpperCase()));
          }
          break;
        }
        case DCH_Day:
          invalidForInterval(isInterval, node);
          if (S_TM(node.suffix) != 0) {
            out.append(days_full[tm.getDayOfWeek()]);
          } else {
            String formatStr = (S_FM(node.suffix) != 0 ? "%s" : "%-9s");
            out.append(String.format(formatStr, days_full[tm.getDayOfWeek()]));
          }
          break;
        case DCH_day:
          invalidForInterval(isInterval, node);
          if (S_TM(node.suffix) != 0) {
            out.append(days_full[tm.getDayOfWeek()].toLowerCase());
          } else {
            String formatStr = (S_FM(node.suffix) != 0 ? "%s" : "%-9s");
            out.append(String.format(formatStr, days_full[tm.getDayOfWeek()].toLowerCase()));
          }
          break;
        case DCH_DY:
          invalidForInterval(isInterval, node);
          if (S_TM(node.suffix) != 0) {
            out.append(days_short[tm.getDayOfWeek()]);
          } else {
            out.append(days_short[tm.getDayOfWeek()]);
          }
          break;
        case DCH_Dy:
          invalidForInterval(isInterval, node);
          if (S_TM(node.suffix) != 0) {
            out.append(days_short[tm.getDayOfWeek()]);
          } else {
            out.append(days_short[tm.getDayOfWeek()]);
          }
          break;
        case DCH_dy:
          invalidForInterval(isInterval, node);
          if (S_TM(node.suffix) != 0) {
            out.append(days_short[tm.getDayOfWeek()]);
          } else {
            out.append(days_short[tm.getDayOfWeek()]);
          }
          break;
        case DCH_DDD:
        case DCH_IDDD: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%0d" : "%03d");
          out.append(String.format(formatStr,
              (node.key.idType == DCH_poz.DCH_DDD) ?
                  tm.getDayOfYear() : DateTimeUtil.date2isoyearday(tm.years, tm.monthOfYear, tm.dayOfMonth)
          ));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_DD: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%02d");
          out.append(String.format(formatStr, tm.dayOfMonth));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_D:
          invalidForInterval(isInterval, node);
          out.append(String.format("%d", tm.getDayOfWeek() + 1));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_ID:
          invalidForInterval(isInterval, node);
          out.append(String.format("%d", (tm.getDayOfWeek() == 0) ? 7 : tm.getDayOfWeek()));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_WW: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%0d" : "%02d");
          out.append(String.format(formatStr, (tm.getDayOfYear() - 1) / 7 + 1));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_IW: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%0d" : "%02d");
          out.append(String.format(formatStr,
              DateTimeUtil.date2isoweek(tm.years, tm.monthOfYear, tm.dayOfMonth)));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_Q:
          if (tm.monthOfYear == 0) {
            break;
          }
          out.append(String.format("%d", (tm.monthOfYear - 1) / 3 + 1));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_CC: {
          if (isInterval) { /* straight calculation */
            i = tm.years / 100;
          } else {
            if (tm.years > 0) {
						/* Century 20 == 1901 - 2000 */
              i = (tm.years - 1) / 100 + 1;
            } else {
						/* Century 6BC == 600BC - 501BC */
              i = tm.years / 100 - 1;
            }
          }
          if (i <= 99 && i >= -99) {
            String formatStr = (S_FM(node.suffix) != 0 ? "%0d" : "%02d");
            out.append(String.format(formatStr, i));
          } else {
            out.append(String.format("%d", i));
          }
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_Y_YYY:
          i = ADJUST_YEAR(tm.years, isInterval) / 1000;
          out.append(String.format("%d,%03d", i, ADJUST_YEAR(tm.years, isInterval) - (i * 1000)));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_YYYY:
        case DCH_IYYY: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%d" : "%04d");
          out.append(String.format(formatStr,
              (node.key.idType == DCH_poz.DCH_YYYY ? ADJUST_YEAR(tm.years, isInterval) :
                  ADJUST_YEAR(DateTimeUtil.date2isoyear(tm.years, tm.monthOfYear, tm.dayOfMonth), isInterval))
          ));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_YYY:
        case DCH_IYY: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%0d" : "%03d");
          out.append(String.format(formatStr,
              (node.key.idType == DCH_poz.DCH_YYY ? ADJUST_YEAR(tm.years, isInterval) :
                  ADJUST_YEAR(DateTimeUtil.date2isoyear(tm.years, tm.monthOfYear, tm.dayOfMonth), isInterval)) % 1000
          ));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        }
        case DCH_YY:
        case DCH_IY: {
          String formatStr = (S_FM(node.suffix) != 0 ? "%0d" : "%02d");
          out.append(String.format(formatStr,
              (node.key.idType == DCH_poz.DCH_YY ? ADJUST_YEAR(tm.years, isInterval) :
                  ADJUST_YEAR(DateTimeUtil.date2isoyear(tm.years, tm.monthOfYear, tm.dayOfMonth), isInterval)) % 100
          ));
          if (S_THth(node.suffix) != 0)
            str_numth(out, out, S_TH_TYPE(node.suffix));
          break;
        }
        case DCH_Y:
        case DCH_I:
          out.append(String.format("%1d",
              (node.key.idType == DCH_poz.DCH_Y ?
                  ADJUST_YEAR(tm.years, isInterval) :
                  ADJUST_YEAR(DateTimeUtil.date2isoyear(tm.years, tm.monthOfYear, tm.dayOfMonth),
                      isInterval)) % 10));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_RM: {
          if (tm.monthOfYear == 0) {
            break;
          }
          String formatStr = (S_FM(node.suffix) != 0 ? "%s" : "%-4s");
          out.append(String.format(formatStr,
              rm_months_upper[MONTHS_PER_YEAR - tm.monthOfYear]));
          break;
        }
        case DCH_rm: {
          if (tm.monthOfYear == 0) {
            break;
          }
          String formatStr = (S_FM(node.suffix) != 0 ? "%s" : "%-4s");
          out.append(String.format(formatStr, rm_months_lower[MONTHS_PER_YEAR - tm.monthOfYear]));
          break;
        }
        case DCH_W:
          out.append(String.format("%d", (tm.dayOfMonth - 1) / 7 + 1));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
        case DCH_J:
          out.append(String.format("%d", DateTimeUtil.date2j(tm.years, tm.monthOfYear, tm.dayOfMonth)));
          if (S_THth(node.suffix) != 0) {
            str_numth(out, out, S_TH_TYPE(node.suffix));
          }
          break;
      }
    }
  }

  /**
   * Return ST/ND/RD/TH for simple (1..9) numbers
   * type --> 0 upper, 1 lower
   * @param num
   * @param type
   * @return
   */
  static String get_th(StringBuilder num, int type) {
    int	len = num.length();
    char last = num.charAt(len - 1);

    if (!Character.isDigit(last)) {
      throw new IllegalArgumentException("\"" + num.toString() + "\" is not a number");
    }

    /*
     * All "teens" (<x>1[0-9]) get 'TH/th', while <x>[02-9][123] still get
     * 'ST/st', 'ND/nd', 'RD/rd', respectively
     */
    char seclast;
    if ((len > 1) && ((seclast = num.charAt(len - 2)) == '1')) {
      last = 0;
    }

    switch (last) {
      case '1':
        if (type == TH_UPPER)
          return numTH[0];
        return numth[0];
      case '2':
        if (type == TH_UPPER)
          return numTH[1];
        return numth[1];
      case '3':
        if (type == TH_UPPER)
          return numTH[2];
        return numth[2];
      default:
        if (type == TH_UPPER)
          return numTH[3];
        return numth[3];
    }
  }

  /**
   * Convert string-number to ordinal string-number
   * type --> 0 upper, 1 lower
   * @param dest
   * @param num
   * @param type
   */
  static void str_numth(StringBuilder dest, StringBuilder num, int type) {
    if (!dest.equals(num)) {
      dest.append(num);
    }
    dest.append(get_th(num, type));
  }

  private static void invalidForInterval(boolean isInterval, FormatNode node) {
    if (isInterval) {
      throw new IllegalArgumentException("\"" + node.key.name + "\" not support for interval");
    }
  }
}
