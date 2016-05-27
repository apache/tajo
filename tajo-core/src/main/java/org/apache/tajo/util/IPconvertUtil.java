/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.tajo.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

public class IPconvertUtil {
  private static final Log LOG = LogFactory.getLog(IPconvertUtil.class);

  public static int ipstr2int(String host) {
    String[] ips = host.split("\\.");

    if (ips.length != 4) {
      LOG.error(host + " is not valid ip string");
      return 0;
    }

    Integer[] ipAddr = Arrays.stream(ips).map(Integer::parseInt).toArray(Integer[]::new);

    int address = ipAddr[3] & 0xFF;
    address |= ((ipAddr[2] << 8) & 0xFF00);
    address |= ((ipAddr[1] << 16) & 0xFF0000);
    address |= ((ipAddr[0] << 24) & 0xFF000000);

    return address;
  }

  public static String int2ipstr(int addr) {
    int[] intAddr = new int[4];

    for (int i = 3; i >= 0; i--) {
      intAddr[i] = addr & 0xFF;
      addr >>= 8;
    }

    return String.format("%d.%d.%d.%d", intAddr[0], intAddr[1], intAddr[2], intAddr[3]);
  }

  public static byte[] ipstr2bytes(String host) {
    String[] ips = host.split("\\.");

    if (ips.length != 4) {
      LOG.error(host + " is not valid ip string");
      return null;
    }

    byte[] result = new byte[4];
    for (int i = 0; i < 4; i++) {
      result[i] = (byte) (Integer.parseInt(ips[i]) & 0xFF);
    }

    return result;
  }

  public static String bytes2ipstr(byte[] addr) {
    return String.format("%d.%d.%d.%d", addr[0] & 0xFF, addr[1] & 0xFF, addr[2] & 0xFF, addr[3] & 0xFF);

  }
}
