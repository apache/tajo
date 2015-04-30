******************************
Python Functions
******************************

=======================
User-defined Functions
=======================

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

  # /path/to/udf1.py

  @output_type('int4')
  def return_one():
    return 1

  @output_type("text")
  def helloworld():
    return 'Hello, World'

  # No decorator - blob
  def concat_py(str):
    return str+str

  @output_type('int4')
  def sum_py(a,b):
    return a+b

If the configuration is set properly, every function in the script files are registered when the Tajo cluster starts up.

-----------------------
Decorators and types
-----------------------

By default, every function has a return type of ``BLOB``.
You can use Python decorators to define output types for the script functions. Tajo can figure out return types from the annotations of the Python script.

* ``output_type``: Defines the return data type for a script UDF in a format that Tajo can understand. The defined type must be one of the types supported by Tajo. For supported types, please refer to :doc:`/sql_language/data_model`.

-----------------------
Query example
-----------------------

Once the Python UDFs are successfully registered, you can use them as other built-in functions.

.. code-block:: sql

  default> select concat_py(n_name)::text from nation where sum_py(n_regionkey,1) > 2;

==============================================
User-defined Aggregation Functions
==============================================

-----------------------
Function registration
-----------------------

To define your Python aggregation functions, you should write Python classes for each function.
Followings are typical examples of Python UDAFs.

.. code-block:: python

  # /path/to/udaf1.py

  class AvgPy:
    sum = 0
    cnt = 0

    def __init__(self):
        self.reset()

    def reset(self):
        self.sum = 0
        self.cnt = 0

    # eval at the first stage
    def eval(self, item):
        self.sum += item
        self.cnt += 1

    # get intermediate result
    def get_partial_result(self):
        return [self.sum, self.cnt]

    # merge intermediate results
    def merge(self, list):
        self.sum += list[0]
        self.cnt += list[1]

    # get final result
    @output_type('float8')
    def get_final_result(self):
        return self.sum / float(self.cnt)


  class CountPy:
    cnt = 0

    def __init__(self):
        self.reset()

    def reset(self):
        self.cnt = 0

    # eval at the first stage
    def eval(self):
        self.cnt += 1

    # get intermediate result
    def get_partial_result(self):
        return self.cnt

    # merge intermediate results
    def merge(self, cnt):
        self.cnt += cnt

    # get final result
    @output_type('int4')
    def get_final_result(self):
        return self.cnt


These classes must provide ``reset()``, ``eval()``, ``merge()``, ``get_partial_result()``, and ``get_final_result()`` functions.

* ``reset()`` resets the aggregation state.
* ``eval()`` evaluates input tuples in the first stage.
* ``merge()`` merges intermediate results of the first stage.
* ``get_partial_result()`` returns intermediate results of the first stage. Output type must be same with the input type of ``merge()``.
* ``get_final_result()`` returns the final aggregation result.

-----------------------
Query example
-----------------------

Once the Python UDAFs are successfully registered, you can use them as other built-in aggregation functions.

.. code-block:: sql

  default> select avgpy(n_nationkey), countpy() from nation;

.. warning::

  Currently, Python UDAFs cannot be used as window functions.