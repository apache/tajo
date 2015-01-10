*****************************
Index (Experimental Feature)
*****************************

An index is a data structure that is used for efficient query processing. Using an index, the Tajo query engine can directly retrieve search values.

This is still an experimental feature. In order to use indexes, you must check out the source code of the ``index_support`` branch::

  git clone -b index_support https://git-wip-us.apache.org/repos/asf/tajo.git tajo-index

For the source code build, please refer to :doc:`getting_started`.

The following sections describe the supported index types, the query execution with an index, and the future works.

.. toctree::
      :maxdepth: 1

      index/types
      index/how_to_use
      index/future_work