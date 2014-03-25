*******************************
Network Functions and Operators
*******************************

.. function:: geoip_country_code (string addr)

  Convert an ipv4 address string to a geoip country code.

  :param string: ipv4 address string
  :rtype: text
  :example:

  .. code-block:: sql

    select geoip_country_code('163.152.71.31')
    > 'KR'

.. function:: geoip_country_code (inet4 addr)

  Convert an ipv4 address to a geoip country code.

  :param string: ipv4 address
  :rtype: text
  :example:

  .. code-block:: sql

    select geoip_country_code(163.152.71.31)
    > 'KR'

.. function:: geoip_in_country (string addr, string code)

  If the given country code is same with the country code of the given address, it returns true. Otherwise, returns false.

  :param addr: ipv4 address string
  :param code: country code
  :rtype: boolean
  :example:

  .. code-block:: sql

    select geoip_in_country('163.152.71.31', 'KR')
    > true

.. function:: geoip_in_country (inet4 addr, string code)

  If the given country code is same with the country code of the given address, it returns true. Otherwise, returns false.

  :param addr: ipv4 address
  :param code: country code
  :rtype: boolean
  :example:

  .. code-block:: sql

    select geoip_in_country(163.152.71.31, 'KR')
    > true