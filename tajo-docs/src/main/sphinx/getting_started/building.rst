*****************
Build source code
*****************

You prepare the prerequisites and the source code, you can build the source code now.

The first step of the installation procedure is to configure the source tree for your system and choose the options you would like. This is done by running the configure script. For a default installation simply enter:

You can compile source code and get a binary archive as follows:

.. code-block:: bash

  $ cd tajo-x.y.z
  $ mvn clean install -DskipTests -Pdist -Dtar
  $ ls tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz

Then, after you move some proper directory, discompress the tar.gz file as follows:

.. code-block:: bash

  $ cd [a directory to be parent of tajo binary]
  $ tar xzvf ${TAJO_SRC}/tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz