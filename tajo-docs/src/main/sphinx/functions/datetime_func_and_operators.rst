********************************
DateTime Functions and Operators
********************************

  * Note : Example result may be various based on time zone.

.. function:: add_days (date DATE|TIMESTAMP, day INT*)

  Returns date value which is added with given day parameter.

  :param date: base timestamp or date
  :param day: day value to be added
  :rtype: TIMESTAMP
  :example:
          
  .. code-block:: sql

    select add_days(date '2013-12-30', 5);
    > 2014-01-03 15:00:00

    select add_days(timestamp '2013-12-05 12:10:20', -7);
    > 2013-11-28 03:10:20

.. function:: add_months (date DATE|TIMESTAMP, month INT*)

  Returns date value which is added with given month parameter.

  :param date: base timestamp or date
  :param month: month value to be added
  :rtype: TIMESTAMP
  :example:
          
  .. code-block:: sql

    select add_months(date '2013-12-17', 2);
    > 2014-02-16 15:00:00

.. function:: current_date ()

  Returns current date.

  :rtype: DATE
          
  .. code-block:: sql

    select current_date();
    > 2014-09-23

.. function:: current_time ()

  Returns current time.

  :rtype: TIME
          
  .. code-block:: sql

    select current_time();
    > 05:18:27.651999

.. function:: extract(field FROM source DATE|TIMESTAMP|TIME)

  The extract function retrieves subfields such as year or hour from date/time values. *source* must be a value expression of type *timestamp*, or *time*. (Expressions of type *date* are cast to *timestamp* and can therefore be used as well.) *field* is an identifier that selects what field to extract from the source value. The extract function returns values of type double precision. The following are valid field names:

  :param field: extract field
  :param source: source value
  :rtype: FLOAT8

  **century**

  The century

  .. code-block:: sql

    select extract(century from timestamp '2001-12-16 12:21:13');
    > 21.0

  The first century starts at 0001-01-01 00:00:00 AD, although they did not know it at the time. This definition applies to all Gregorian calendar countries. There is no century number 0, you go from -1 century to 1 century. If you disagree with this, please write your complaint to: Pope, Cathedral Saint-Peter of Roma, Vatican.

  **day**

  For *timestamp* values, the day (of the month) field (1 - 31)

  .. code-block:: sql

    select extract(day from timestamp '2001-02-16 20:38:40');
    > 16.0

  **decade**

  The year field divided by 10

  .. code-block:: sql

    select extract(decade from timestamp '2001-02-16 20:38:40');
    > 200.0

  **dow**

  The day of the week as Sunday(0) to Saturday(6)

  .. code-block:: sql

    select extract(dow from timestamp '2001-02-16 20:38:40');
    > 5.0

  Note that extract's day of the week numbering differs from that of the to_char(..., 'D') function.

  **doy**

  The day of the year (1 - 365/366)

  .. code-block:: sql

    select extract(doy from timestamp '2001-02-16 20:38:40');
    > 47.0

  **hour**

  The hour field (0 - 23)

  .. code-block:: sql

    select extract(hour from timestamp '2001-02-16 20:38:40');
    > 20.0

  **isodow**

  The day of the week as Monday(1) to Sunday(7)

  .. code-block:: sql

    select extract(isodow from timestamp '2001-02-18 20:38:40');
    > 7.0

  This is identical to dow except for Sunday. This matches the ISO 8601 day of the week numbering.

  **isoyear**

  The ISO 8601 year that the date falls in

  .. code-block:: sql

    select extract(isoyear from date '2006-01-01');
    > 2005.0

  Each ISO year begins with the Monday of the week containing the 4th of January, so in early January or late December the ISO year may be different from the Gregorian year. See the week field for more information.

  **microseconds**

  The seconds field, including fractional parts, multiplied by 1 000 000; note that this includes full seconds

  .. code-block:: sql

    select extract(microseconds from time '17:12:28.5');
    > 2.85E7

  **millennium**

  The millennium

  .. code-block:: sql

    select extract(millennium from timestamp '2001-02-16 20:38:40');
    > 3.0

  Years in the 1900s are in the second millennium. The third millennium started January 1, 2001.

  **milliseconds**

  The seconds field, including fractional parts, multiplied by 1000. Note that this includes full seconds.

  .. code-block:: sql

    select extract(milliseconds from time '17:12:28.5');
    > 28500.0

  **minute**

  The minutes field (0 - 59)

  .. code-block:: sql

    select extract(minute from timestamp '2001-02-16 20:38:40');
    > 38.0

  **month**

  For timestamp values, the number of the month within the year (1 - 12)

  .. code-block:: sql

    select extract(month from timestamp '2001-02-16 20:38:40');
    > 2.0

  **quarter**

  The quarter of the year (1 - 4) that the date is in

  .. code-block:: sql

    select extract(quarter from timestamp '2001-02-16 20:38:40');
    > 1.0

  **second**

  The seconds field, including fractional parts (0 - 59[1])

  .. code-block:: sql

    select extract(second from timestamp '2001-02-16 20:38:40');
    > 40.0

  **week**

  The number of the week of the year that the day is in. By definition (ISO 8601), weeks start on Mondays and the first week of a year contains January 4 of that year. In other words, the first Thursday of a year is in week 1 of that year.

  In the ISO definition, it is possible for early-January dates to be part of the 52nd or 53rd week of the previous year, and for late-December dates to be part of the first week of the next year. For example, 2005-01-01 is part of the 53rd week of year 2004, and 2006-01-01 is part of the 52nd week of year 2005, while 2012-12-31 is part of the first week of 2013. It's recommended to use the isoyear field together with week to get consistent results.

  .. code-block:: sql

    select extract(week from timestamp '2001-02-16 20:38:40');
    > 7.0

  **year**

  The year field. Keep in mind there is no 0 AD, so subtracting BC years from AD years should be done with care.

  .. code-block:: sql

    select extract(year from timestamp '2001-02-16 20:38:40');
    > 2001.0

  The extract function is primarily intended for computational processing.

  The date_part function is also supported. It is equivalent to the SQL-standard function extract:

.. function:: date_part(field TEXT, source DATE|TIMESTAMP|TIME)

  Note that here the field parameter needs to be a string value, not a name. The valid field names for date_part are the same as for extract.

  :param field: extract field
  :param source: source value
  :rtype: FLOAT8

  .. code-block:: sql

    select date_part('day', timestamp '2001-02-16 20:38:40');
    > 16.0

.. function:: now()

  Returns current timestamp

  :rtype: TIMESTAMP
  :example:

  .. code-block:: sql

    select now();
    > 2014-09-23 08:32:43.286

.. function:: to_char(src TIMESTAMP, format TEXT)

  Converts timestamp to text. For more detailed, see 'Date/Time Formatting and Conversion' section below.

  :param src: timestamp to be converted
  :param format: format string
  :rtype: TEXT

  .. code-block:: sql

    select to_char(current_timestamp, 'yyyy-MM-dd');
    > 2014-09-23

.. function:: to_date(src TEXT, format TEXT)

  Converts text to date. For more detailed, see 'Date/Time Formatting and Conversion' section below.

  :param src: date string to be converted
  :param format: format string
  :rtype: DATE

  .. code-block:: sql

    select to_date('2014-01-04', 'YYYY-MM-DD');
    > 2014-01-04

.. function:: to_timestamp(epoch INT*)

  Converts int(UNIX epoch) to timestamp.

  :param epoch: second value from Jan. 1, 1970
  :rtype: TIMESTAMP

  .. code-block:: sql

    select to_timestamp(412312345);
    > 1983-01-25 03:12:25

.. function:: to_timestamp(src TEXT, format TEXT)

  Converts text timestamp. For more detailed, see 'Date/Time Formatting and Conversion' section below.

  :param src: timestamp string to be converted
  :param format: format string
  :rtype: TIMESTAMP

  .. code-block:: sql

    select to_timestamp('97/2/16 8:14:30', 'FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS');
    > 0097-02-15 23:14:30

.. function:: utc_usec_to (string TEXT , timestamp INT8 [, dayOfWeek INT4])

  * If the **first parameter** is 'day'.

    Shifts and return a UNIX timestamp in microseconds to the beginning of the day it occurs in.
    For example, if unix_timestamp occurs on May 19th at 08:58, this function returns a UNIX timestamp for May 19th at 00:00 (midnight).

  * If the **first parameter** is 'hour'.

    Shifts and return a UNIX timestamp in microseconds to the beginning of the hour it occurs in.
    For example, if unix_timestamp occurs at 08:58, this function returns a UNIX timestamp for 08:00 on the same day.

  * If the **first parameter** is 'month'.

    Shifts and return a UNIX timestamp in microseconds to the beginning of the month it occurs in.
    For example, if unix_timestamp occurs on March 19th, this function returns a UNIX timestamp for March 1st of the same year.

  * If the **first parameter** is 'year'.

    Returns a UNIX timestamp in microseconds that represents the year of the unix_timestamp argument.
    For example, if unix_timestamp occurs in 2010, the function returns 1274259481071200, the microsecond representation of 2010-01-01 00:00.

  * If the **first parameter** is 'week' and **third parameter** is 2 i.e (TUESDAY)

    Returns a UNIX timestamp in microseconds that represents a day in the week of the
    For example, if unix_timestamp occurs on Friday, 2008-04-11, and you set day_of_week to 2 (Tuesday), the function returns a UNIX timestamp for Tuesday, 2008-04-08.

  :param string: could be one of 'day', 'hour', 'month', 'year' and 'week'
  :param timestamp: unix timestamp in microseconds
  :param dayOfWeek: day of the week from 0 (Sunday) to 6 (Saturday). This is optional parameter required only if first parameter is 'week'
  :rtype: INT8
  :example:

  .. code-block:: sql

    SELECT utc_usec_to('day', 1274259481071200);
    > 1274227200000000


.. note:: ``INT*`` means various size of integer types can be accepted. And ``FLOAT*`` means both of ``FLOAT4`` and ``FLOAT8`` are OK.
    For your information, in Tajo, ``INT`` is alias for ``INT4`` and ``FLOAT`` is one for ``FLOAT4``.
    See :doc:`/sql_language/data_model` .

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Date/Time Formatting and Conversion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*Template patterns for date/time formatting*

=========================== ================================================================
Pattern                     Description
=========================== ================================================================
HH                          hour of day (01-12)
HH12                        hour of day (01-12)
HH24                        hour of day (00-23)
MI                          minute (00-59)
SS                          second (00-59)
MS                          millisecond (000-999)
US                          microsecond (000000-999999)
SSSS                        seconds past midnight (0-86399)
AM, am, PM or pm            meridiem indicator (without periods)
A.M., a.m., P.M. or p.m.    meridiem indicator (with periods)
Y,YYY                       year (4 and more digits) with comma
YYYY                        year (4 and more digits)
YYY                         last 3 digits of year
YY                          last 2 digits of year
Y                           last digit of year
IYYY                        ISO year (4 and more digits)
IYY                         last 3 digits of ISO year
IY                          last 2 digits of ISO year
I                           last digit of ISO year
BC, bc, AD or ad            era indicator (without periods)
B.C., b.c., A.D. or a.d.    era indicator (with periods)
MONTH                       full upper case month name (blank-padded to 9 chars)
Month                       full capitalized month name (blank-padded to 9 chars)
month                       full lower case month name (blank-padded to 9 chars)
MON                         abbreviated upper case month name (3 chars in English, localized lengths vary)
Mon                         abbreviated capitalized month name (3 chars in English, localized lengths vary)
mon                         abbreviated lower case month name (3 chars in English, localized lengths vary)
MM                          month number (01-12)
DAY                         full upper case day name (blank-padded to 9 chars)
Day                         full capitalized day name (blank-padded to 9 chars)
day                         full lower case day name (blank-padded to 9 chars)
DY                          abbreviated upper case day name (3 chars in English, localized lengths vary)
Dy                          abbreviated capitalized day name (3 chars in English, localized lengths vary)
dy                          abbreviated lower case day name (3 chars in English, localized lengths vary)
DDD                         day of year (001-366)
IDDD                        ISO day of year (001-371; day 1 of the year is Monday of the first ISO week.)
DD                          day of month (01-31)
D                           day of the week, Sunday(1) to Saturday(7)
ID                          ISO day of the week, Monday(1) to Sunday(7)
W                           week of month (1-5) (The first week starts on the first day of the month.)
WW                          week number of year (1-53) (The first week starts on the first day of the year.)
IW                          ISO week number of year (01 - 53; the first Thursday of the new year is in week 1.)
CC                          century (2 digits) (The twenty-first century starts on 2001-01-01.)
J                           Julian Day (integer days since November 24, 4714 BC at midnight UTC)
Q                           quarter (ignored by to_date and to_timestamp)
RM                          month in upper case Roman numerals (I-XII; I=January)
rm                          month in lower case Roman numerals (i-xii; i=January)
TZ                          upper case time-zone name
tz                          lower case time-zone name
=========================== ================================================================


*Template pattern modifiers for date/time formatting*

=========== ======================================================================= ================
Modifier    Description                                                             Example
=========== ======================================================================= ================
FM prefix   fill mode (suppress padding blanks and trailing zeroes)                 FMMonth
TH suffix   upper case ordinal number suffix    DDTH, e.g.,                         12TH
th suffix   lower case ordinal number suffix    DDth, e.g.,                         12th
FX prefix   fixed format global option (see usage notes)                            FX Month DD Day
TM prefix   translation mode (print localized day and month names based on lc_time) TMMonth
SP suffix   spell mode (not implemented)                                            DDSP
=========== ======================================================================= ================

  * FM suppresses leading zeroes and trailing blanks that would otherwise be added to make the output of a pattern be fixed-width. In Tajo, FM modifies only the next specification, while in Oracle FM affects all subsequent specifications, and repeated FM modifiers toggle fill mode on and off.

  * TM does not include trailing blanks.

  * *to_timestamp* and *to_date* skip multiple blank spaces in the input string unless the FX option is used. For example, *to_timestamp* ('2000    JUN', 'YYYY MON') works, but *to_timestamp* ('2000    JUN', 'FXYYYY MON') returns an error because *to_timestamp* expects one space only. FX must be specified as the first item in the template.

  * Ordinary text is allowed in *to_char* templates and will be output literally. You can put a substring in double quotes to force it to be interpreted as literal text even if it contains pattern key words. For example, in '"Hello Year "YYYY', the YYYY will be replaced by the year data, but the single Y in Year will not be. In *to_date*, to_number, and *to_timestamp*, double-quoted strings skip the number of input characters contained in the string, e.g. "XX" skips two input characters.

  * If you want to have a double quote in the output you must precede it with a backslash, for example '\"YYYY Month\"'.

  * If the year format specification is less than four digits, e.g. YYY, and the supplied year is less than four digits, the year will be adjusted to be nearest to the year 2020, e.g. 95 becomes 1995.

  * The YYYY conversion from string to timestamp or date has a restriction when processing years with more than 4 digits. You must use some non-digit character or template after YYYY, otherwise the year is always interpreted as 4 digits. For example (with the year 20000): *to_date* ('200001131', 'YYYYMMDD') will be interpreted as a 4-digit year; instead use a non-digit separator after the year, like *to_date* ('20000-1131', 'YYYY-MMDD') or *to_date* ('20000Nov31', 'YYYYMonDD').

  * In conversions from string to timestamp or date, the CC (century) field is ignored if there is a YYY, YYYY or Y,YYY field. If CC is used with YY or Y then the year is computed as the year in the specified century. If the century is specified but the year is not, the first year of the century is assumed.

  * An ISO week date (as distinct from a Gregorian date) can be specified to *to_timestamp* and *to_date* in one of two ways:

  * Year, week, and weekday: for example *to_date* ('2006-42-4', 'IYYY-IW-ID') returns the date 2006-10-19. If you omit the weekday it is assumed to be 1 (Monday).

  * Year and day of year: for example *to_date* ('2006-291', 'IYYY-IDDD') also returns 2006-10-19.

  * Attempting to construct a date using a mixture of ISO week and Gregorian date fields is nonsensical, and will cause an error. In the context of an ISO year, the concept of a "month" or "day of month" has no meaning. In the context of a Gregorian year, the ISO week has no meaning. Users should avoid mixing Gregorian and ISO date specifications.

  * In a conversion from string to timestamp, millisecond (MS) or microsecond (US) values are used as the seconds digits after the decimal point. For example *to_timestamp* ('12:3', 'SS:MS') is not 3 milliseconds, but 300, because the conversion counts it as 12 + 0.3 seconds. This means for the format SS:MS, the input values 12:3, 12:30, and 12:300 specify the same number of milliseconds. To get three milliseconds, one must use 12:003, which the conversion counts as 12 + 0.003 = 12.003 seconds.

  * Here is a more complex example: *to_timestamp* ('15:12:02.020.001230', 'HH:MI:SS.MS.US') is 15 hours, 12 minutes, and 2 seconds + 20 milliseconds + 1230 microseconds = 2.021230 seconds.

  * *to_char* (..., 'ID')'s day of the week numbering matches the extract(isodow from ...) function, but *to_char* (..., 'D')'s does not match extract(dow from ...)'s day numbering.

  * *to_char* (interval) formats HH and HH12 as shown on a 12-hour clock, i.e. zero hours and 36 hours output as 12, while HH24 outputs the full hour value, which can exceed 23 for intervals.
