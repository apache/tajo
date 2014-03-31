********************************
DateTime Functions and Operators
********************************

.. function:: utc_usec_to (string text , long timestamp , int dayOfWeek)

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

  :param string: could be 'day' 'hour' 'month' 'year' 'week'
  :param long: unix timestamp in microseconds
  :param int: day of the week from 0 (Sunday) to 6 (Saturday).Optional parameter required only if first parameter is 'week'
  :rtype: long
  :alias: utc_usec_to
  :example:

  .. code-block:: sql

    SELECT utc_usec_to('day', 1274259481071200);
    > 1274227200000000