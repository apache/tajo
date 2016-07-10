****************************
Math Functions and Operators
****************************

.. function:: abs (number INT*|FLOAT*)

  Returns absolute value

  :param number: input number
  :rtype: same as a parameter type
  :example:
  
  .. code-block:: sql

    select abs(-9); 
    > 9

.. function:: acos (number FLOAT*)

  Returns the arc cosine of number value

  :param number: input number as radian
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select acos(0.3); 
    > 1.2661036727794992 

.. function:: asin (number FLOAT*)

  Returns the arc sine of number value

  :param number: input number as radian
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select acos(0.8); 
    > 0.9272952180016123

.. function:: atan (number FLOAT8)

  Returns the arc tangent of number value

  :param number: input number as radian
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select atan(0.8); 
    > 0.6747409422235527

.. function:: atan2 (y FLOAT*, x FLOAT*)

  Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta)

  :param y: the ordinate(y axis) coordinate
  :param x: the abscissa(x axis) coordinate
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select atan2(2.7, 0.3);
    > 1.460139105621001

.. function:: cbrt (number FLOAT*)

  Returns the cube root of a number

  :param number: target real number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select cbrt(27.0); 
    > 3.0

.. function:: ceil (number FLOAT*)

  Returns a smallest integer not less than argument

  :param number: target real number
  :rtype: INT8
  :alias: ceiling
  :example:

  .. code-block:: sql

    select ceil(-42.8); 
    > -42

.. function:: cos (number FLOAT*)

  Returns the cosine of a number

  :param number: target real number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select cos(0.7);
    > 0.7648421872844885

.. function:: degrees (number FLOAT*)

  Converts radians to degrees

  :param number: radian value
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select degrees(0.8);
    > 45.83662361046586

.. function:: div (num1 INT*, num2 INT*)

  Integer division truncates resut

  :param num1: number to be divided
  :param num2: number to divide
  :rtype: INT8
  :example:

  .. code-block:: sql

    select div(8,3);
    > 2

.. function:: exp (number FLOAT*)

  Returns Euler's number e raised to the power of a number

  :param number: input number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select exp(1.0);
    > 2.718281828459045

.. function:: floor (number FLOAT*)

  Returns a largest integer not greater than argument

  :param number: target real number
  :rtype: INT8
  :example:

  .. code-block:: sql

    select floor(53.1); 
    > 53

.. function:: mod (num1 INT*, num2 INT*)

  Returns remainder of num1 / num2

  :param num1: number to be divided
  :param num2: number to divide
  :rtype: INT8
  :example:

  .. code-block:: sql

    select mod(10,3);
    > 1

.. function:: pi ()

  Returns constant value of pi

  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select pi();
    > 3.141592653589793

.. function:: pow (x FLOAT*, y FLOAT*)

  Returns value of x raised to the power of y

  :param x: base number
  :param y: exponent
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select pow(2.0, 10.0);
    > 1024.0

.. function:: radians (number FLOAT*)

  Converts degrees to radians

  :param number: degree value
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select radians(45.0);
    > 0.7853981633974483

.. function:: random(number INT4)

  Returns a pseudorandom number.

  :param number: range restriction
  :rtype: INT4
  :example:

  .. code-block:: sql

    select random(10);
    > 4

.. function:: round (number INT*|FLOAT*)

  Rounds to nearest integer

  :param number: target number
  :rtype: INT8
  :example:

  .. code-block:: sql

    select round(5.1); 
    > 5

.. function:: sign (number INT*|FLOAT*)

  Returns sign of argument as -1, 0, 1

  :param number: target number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select sign(-8.4); 
    > -1.0

.. function:: SIN (number FLOAT*)

  Returns the sine of number value

  :param number: target number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select sin(1.0); 
    > 0.8414709848078965

.. function:: sqrt (number FLOAT8)

  Returns the square root of a number

  :param number: target number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select sqrt(256.0); 
    > 16.0

.. function:: tan (number FLOAT*)

  Returns the tangent of number value

  :param number: target number
  :rtype: FLOAT8
  :example:

  .. code-block:: sql

    select tan(0.2); 
    > 0.2027100355086725


.. note:: ``INT*`` means various size of integer types can be accepted. And ``FLOAT*`` means both of ``FLOAT4`` and ``FLOAT8`` are OK.
    For your information, in Tajo SQL, ``INT`` is alias for ``INT4`` and ``FLOAT`` is one for ``FLOAT4``.
    See :doc:`/sql_language/data_model` .
