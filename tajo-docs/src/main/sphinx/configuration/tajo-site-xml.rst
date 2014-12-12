**********************
The tajo-site.xml File
**********************

To the ``core-site.xml`` file on every host in your cluster, you must add the following information:

======================
System Config
======================



======================
Date/Time Settings
======================

+--------------------------+----------------+--------------------------------------------------------+
| Property Name            | Property Value | Descriptions                                           |
+==========================+================+========================================================+
| tajo.timezone            | Time zone id   | Refer to :doc:`/time_zone`                             |
+--------------------------+----------------+--------------------------------------------------------+
| tajo.datetime.date-order | Date order     | Determine date order. It should be one of YMD, DMY, MDY|
+--------------------------+----------------+--------------------------------------------------------+

