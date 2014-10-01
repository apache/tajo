*****************************
Math Functions and Operators
*****************************

.. function:: abs (number int|float)

  Returns absolute value

  :param number: input number
  :rtype: same as a parameter type
  :example:
  
  .. code-block:: sql

    select abs(-9); 
    > 9

.. function:: acos (number float)

  Returns the arc cosine of number value

  :param number: input number as radian
  :rtype: float8
  :example:

  .. code-block:: sql

    select acos(0.3); 
    > 1.2661036727794992 

.. function:: asin (number float)

  Returns the arc sine of number value

  :param number: input number as radian
  :rtype: float8
  :example:

  .. code-block:: sql

    select acos(0.8); 
    > 0.9272952180016123

.. function:: atan (number float8)

  Returns the arc tangent of number value

  :param number: input number as radian
  :rtype: float8
  :example:

  .. code-block:: sql

    select atan(0.8); 
    > 0.6747409422235527

.. function:: atan2 (y float, x float)

  Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta)

  :param y: the ordinate(y axis) coordinate
  :param x: the abscissa(x axis) coordinate
  :rtype: float8
  :example:

  .. code-block:: sql

    select atan2(2.7, 0.3);
    > 1.460139105621001

.. function:: cbrt (number float)

  Returns the cube root of a number

  :param number: target real number
  :rtype: float8
  :example:

  .. code-block:: sql

    select cbrt(27.0); 
    > 3.0

.. function:: ceil (number float)

  Returns a smallest integer not less than argument

  :param number: target real number
  :rtype: int8
  :example:

  .. code-block:: sql

    select ceil(-42.8); 
    > -42

.. function:: cos (number float)

  Returns the cosine of a number

  :param number: target real number
  :rtype: float8
  :example:

  .. code-block:: sql

    select cos(0.7);
    > 0.7648421872844885

.. function:: degrees (number float)

  Converts radians to degrees

  :param number: radian value
  :rtype: float8
  :example:

  .. code-block:: sql

    select degrees(0.8);
    > 45.83662361046586

.. function:: div (num1 int, num2 int)

  Integer division truncates resut

  :param num1: number to be divided
  :param num2: number to divide
  :rtype: int8
  :example:

  .. code-block:: sql

    select div(8,3);
    > 2

.. function:: exp (number float)

  Returns Euler's number e raised to the power of a number

  :param number: input number
  :rtype: float8
  :example:

  .. code-block:: sql

    select exp(1.0);
    > 2.718281828459045

.. function:: floor (number float)

  Returns a largest integer not greater than argument

  :param number: target real number
  :rtype: int8
  :example:

  .. code-block:: sql

    select floor(53.1); 
    > 53

.. function:: mod (num1 int, num2 int)

  Returns remainder of num1 / num2

  :param num1: number to be divided
  :param num2: number to divide
  :rtype: int8
  :example:

  .. code-block:: sql

    select mod(10,3);
    > 1

.. function:: pi ()

  Returns constant value of pi

  :rtype: float8
  :example:

  .. code-block:: sql

    select pi();
    > 3.141592653589793

.. function:: pow (x float, y float)

  Returns value of x raised to the power of y

  :param x: base number
  :param y: exponent
  :rtype: float8
  :example:

  .. code-block:: sql

    select pow(2.0, 10.0);
    > 1024.0

.. function:: radians (number float)

  Converts degrees to radians

  :param number: degree value
  :rtype: float8
  :example:

  .. code-block:: sql

    select radians(45.0);
    > 0.7853981633974483

.. function:: round (number int|float)

  Rounds to nearest integer

  :param number: target number
  :rtype: int8
  :example:

  .. code-block:: sql

    select round(5.1); 
    > 5

.. function:: sign (number int|float)

  Returns sign of argument as -1, 0, 1

  :param number: target number
  :rtype: float8
  :example:

  .. code-block:: sql

    select sign(-8.4); 
    > -1.0

.. function:: sin (number float)

  Returns the sine of number value

  :param number: target number
  :rtype: float8
  :example:

  .. code-block:: sql

    select sin(1.0); 
    > 0.8414709848078965

.. function:: sqrt (number float8)

  Returns the square root of a number

  :param number: target number
  :rtype: float8
  :example:

  .. code-block:: sql

    select sqrt(256.0); 
    > 16.0

.. function:: tan (number float)

  Returns the tangent of number value

  :param number: target number
  :rtype: float8
  :example:

  .. code-block:: sql

    select tan(0.2); 
    > 0.2027100355086725
