***********
Direct Output Committer
***********

The final output of a Tajo query is written to a location in S3 or HDFS. However, each task that is supposed to write the output data into a final location first writes it into a temporary location. It is in a task’s commit phase, when the output data is moved from the temporary location to a final location.

DirectOutputCommitter is a setting, which when enabled lets a task write the output data directly to the final location. In this case, a commit phase just will backup previous output data. Setting DirectOutputCommitter avoids Eventual Consistency issues and its default value is false.

===========================================
Properties for Direct Output Committer
===========================================

You can set DirectOutputCommitter in conf/tajo-site.xml file as follows:

.. code-block:: xml

  <name>tajo.query.direct-output-committer.enabled</name>
  <value>true</value>


Or you can set DirectOutputCommitter with ``\set`` command as follows:

.. code-block:: sh

  \set DIRECT_OUTPUT_COMMITTER_ENABLED true