******************
Functions
******************

Tajo provides extensive supports for functions. It includes a lot of built-in functions and user-defined functions which is implemented in Python.

===================
Built-in Functions
===================

.. toctree::
    :maxdepth: 1

    functions/math_func_and_operators
    functions/string_func_and_operators
    functions/datetime_func_and_operators
    functions/network_func_and_operators
    functions/json_func

==============================
Python User-defined Functions
==============================

-----------------------
Function registration
-----------------------

To register Python UDFs, you must install script files in all cluster nodes.
After that, you can register your functions by specifying the paths to those script files in ``tajo-site.xml``. Here is an example of the configuration.

.. code-block:: xml

  <property>
    <name>tajo.function.python.code-dir</name>
    <value>/path/to/script1.py,/path/to/script2.py</value>
  </property>

Please note that you can specify multiple paths with ``','`` as a delimiter. Each file can contain multiple functions. Here is a typical example of a script file.

.. code-block:: python

  # /path/to/script1.py

  @outputType('int4')
  def return_one():
    return 1

  @outputType("text")
  def helloworld():
    return 'Hello, World'

  # No decorator - blob
  def concat_py(str):
    return str+str

  @outputType('int4')
  def sum_py(a,b):
    return a+b

If the configuration is set properly, every function in the script files are registered when the Tajo cluster starts up.

-----------------------
Decorators and types
-----------------------

By default, every function has a return type of ``BLOB``.
You can use Python decorators to define output types for the script functions. Tajo can figure out return types from the annotations of the Python script.

* ``outputType``: Defines the return data type for a script UDF in a format that Tajo can understand. The defined type must be one of the types supported by Tajo. For supported types, please refer to :doc:`/sql_language/data_model`.

-----------------------
Query example
-----------------------

Once the Python UDFs are successfully registered, you can use them as other built-in functions.

.. code-block:: sql

  default> select concat_py(n_name)::text from nation where sum_py(n_regionkey,1) > 2;

