*******************************
Network Functions and Operators
*******************************

=============
Prerequisites
=============

Apache Tajo provides network functions and operations using GeoIP databases.
To use these functions and operations, the GeoIP database should be precedently installed in local disks of
all the workers.
(Please refer the install instruction in http://dev.maxmind.com/geoip/legacy/downloadable/)

Once the GeoIP database is installed, you should specify the install location in ``conf/tajo-site.xml``
as follows. ::

  <property>
    <name>tajo.function.geoip-database-location</name>
    <value>/path/to/geoip/database/file</value>
  </property>

===================
Supported Functions
===================

.. function:: geoip_country_code (addr TEXT|INT*|BINARY|BLOB)

  Convert an ipv4 address string to a geoip country code.

  :param addr: ipv4 address string (also accepts integer or blob type representation)
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select geoip_country_code('163.152.71.31')
    > 'KR'

.. function:: geoip_in_country (addr TEXT|INT*|BINARY|BLOB, code TEXT)

  If the given country code is same with the country code of the given address, it returns true. Otherwise, returns false.

  :param addr: ipv4 address string (also accepts integer or blob type representation)
  :param code: country code
  :rtype: BOOLEAN
  :example:

  .. code-block:: sql

    select geoip_in_country('163.152.71.31', 'KR')
    > true


=================
Utility Functions
=================

.. function:: ipstr_to_int (addr TEXT)

  Convert string for ipv4 address to INT4 type

  :param addr: ipv4 address string
  :rtype: INT4
  :example:

  .. code-block:: sql

    SELECT ipstr_to_int('1.2.3.4')
    > 16909060


.. function:: int_to_ipstr (addr INT4)

  Convert an INT4 type value to ipv4 string

  :param addr: ipv4 address as integer representation
  :rtype: TEXT
  :example:

  .. code-block:: sql

    SELECT int_to_ipstr(16909060)
    > '1.2.3.4'


.. function:: ipstr_to_blob (addr TEXT)

  Convert string for ipv4 address to BINARY type

  :param addr: ipv4 address string
  :rtype: BINARY


.. function:: bin_to_ipstr (bin_ip BINARY|BLOB)

  Convert a binary type value to ipv4 string

  :param bin_ip: binary represetion for ipv4 address
  :rtype: TEXT
  :example:

  .. code-block:: sql

    SELECT bin_to_ipstr(ipstr_to_blob('1.2.3.4'))
    > '1.2.3.4'
