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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

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

  /** Converts int value to country code.
   *
   *  @param host ip address as int type by network byte order(big endian)
   */
  public static String getCountryCode(int host) {
      return lookup.getCountry(host).getCode();
  }

  /** Converts binary(byte array) to country code.
   *  In case of IPv4, it is 4 bytes length.
   *
   *  @param host ip address as byte array type by network byte order(big endian)
   */
  public static String getCountryCode(byte [] host) {
    InetAddress addr;

    try {
      addr = InetAddress.getByAddress(host);
    } catch (UnknownHostException e) {
      LOG.error("Unknown host: "+host);
      return "";
    }

    return lookup.getCountry(addr).getCode();
  }

  public static int ipstr2int(String host) {
    String [] ips = host.split("\\.");

    if (ips.length != 4) {
      LOG.error(host+" is not valid ip string");
      return 0;
    }

    Integer [] ipAddr = Arrays.stream(ips).map(Integer::parseInt).toArray(Integer[]::new);

    int address  = ipAddr[3] & 0xFF;
    address |= ((ipAddr[2] << 8) & 0xFF00);
    address |= ((ipAddr[1] << 16) & 0xFF0000);
    address |= ((ipAddr[0] << 24) & 0xFF000000);

    return address;
  }

  public static String int2ipstr(int addr) {
    StringBuilder host = new StringBuilder();

    int [] intAddr = new int [4];

    for (int i=3; i>=0; i--) {
      intAddr[i] = addr & 0xFF;
      addr >>= 8;
    }

    return String.format("%d.%d.%d.%d", intAddr[0], intAddr[1], intAddr[2], intAddr[3]);
  }
}
