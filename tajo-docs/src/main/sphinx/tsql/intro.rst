*****************************
Introducing to TSQL
*****************************

==========
Synopsis
==========

.. code-block:: bash

  bin/tsql [options] [database name]

If a *database_name* is given, tsql connects to the database at startup time. Otherwise, tsql connects to ``default`` database.

Options

* ``-c "quoted sql"`` : Execute quoted sql statements, and then the shell will exist.
* ``-f filename (--file filename)`` : Use the file named filename as the source of commands instead of interactive shell.
* ``-h hostname (--host hostname)`` : Specifies the host name of the machine on which the Tajo master is running.
* ``-p port (--port port)`` : Specifies the TCP port. If it is not set, the port will be 26002 by default.
* ``-conf configuration (--conf configuration)`` : Setting tajo configuration value.
* ``-param parameter (--param parameter)`` : Use a parameter value in SQL file.
* ``-B (--background)`` : Execute as background process.

===================
Entering tsql shell
===================

If the hostname and the port num are not given, tsql will try to connect the Tajo master specified in ${TAJO_HOME}/conf/tajo-site.xml. ::

  bin/tsql

  default>

If you want to connect a specified TajoMaster, you should use '-h' and (or) 'p' options as follows: ::

  bin/tsql -h localhost -p 9004

  default>

The prompt indicates the current database.
