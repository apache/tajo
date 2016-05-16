/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util;

import com.maxmind.geoip.LookupService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;

import java.io.IOException;

public class GeoIPUtil {
  private static final Log LOG = LogFactory.getLog(GeoIPUtil.class);
  private static LookupService lookup;

  static {
    try {
      TajoConf conf = new TajoConf();
      lookup = new LookupService(conf.getVar(ConfVars.GEOIP_DATA),
          LookupService.GEOIP_MEMORY_CACHE);
    } catch (IOException e) {
      LOG.error("Cannot open the geoip data", e);
    }
  }

  public static String getCountryCode(String host) {
    return lookup.getCountry(host).getCode();
  }

  /** Converts long(or int) value to country code.
   *  In case of IPv4, only 4 bytes of long type variable are used.
   *
   *  @param host ip address as long(int) type by network byte order(big endian)
   */
  public static String getCountryCode(long host) {
    return lookup.getCountry(host).getCode();
  }

  /** Converts binary(byte array) to country code.
   *  In case of IPv4, it is 4 bytes length.
   *
   *  @param host ip address as byte array type by network byte order(big endian)
   */
  public static String getCountryCode(byte [] host) {
    return lookup.getCountry(bytesToLong(host)).getCode();
  }

  // It's from geoip-api code.
  private static long bytesToLong(byte[] address) {
    long ipnum = 0;
    for (int i = 0; i < 4; ++i) {
      long y = address[i];
      if (y < 0) {
        y += 256;
      }
      ipnum += y << ((3 - i) * 8);
    }
    return ipnum;
  }
}
