*********************************
Executing HDFS commands
*********************************

You can run the hadoop dfs command(FsShell) within tsql. ``\dfs`` command provides a shortcut to the hadoop dfs commands. If you want to use this command, just specify FsShell arguments and add the semicolon at the end as follows:

.. code-block:: sql

  default> \dfs -ls /;
  Found 3 items
  drwxr-xr-x   - tajo supergroup          0 2014-08-14 04:04 /tajo
  drwxr-xr-x   - tajo supergroup          0 2014-09-04 02:20 /tmp
  drwxr-xr-x   - tajo supergroup          0 2014-09-16 13:41 /user

  default> \dfs -ls /tajo;
  Found 2 items
  drwxr-xr-x   - tajo supergroup          0 2014-08-14 04:04 /tajo/system
  drwxr-xr-x   - tajo supergroup          0 2014-08-14 04:15 /tajo/warehouse

  default> \dfs -mkdir /tajo/temp;

  default> \dfs -ls /tajo;
  Found 3 items
  drwxr-xr-x   - tajo supergroup          0 2014-08-14 04:04 /tajo/system
  drwxr-xr-x   - tajo supergroup          0 2014-09-23 06:48 /tajo/temp
  drwxr-xr-x   - tajo supergroup          0 2014-08-14 04:15 /tajo/warehouse