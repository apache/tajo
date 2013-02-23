/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachedDNSResolver {
  private static Map<String, String> hostNameToIPAddrMap
      = new ConcurrentHashMap<String, String>();

  private static CachedDNSResolver instance;

  static {
    instance = new CachedDNSResolver();
  }

  public static String resolve(String hostName) {

    if (hostNameToIPAddrMap.containsKey(hostName)) {
      return hostNameToIPAddrMap.get(hostName);
    }

    String ipAddress = null;
    try {
      ipAddress = InetAddress.getByName(hostName).getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    hostNameToIPAddrMap.put(hostName, ipAddress);

    return ipAddress;
  }

  public static String [] resolve(String [] hostNames) {
    if (hostNames == null) {
      return null;
    }

    String [] resolved = new String[hostNames.length];
    for (int i = 0; i < hostNames.length; i++) {
      resolved[i] = resolve(hostNames[i]);
    }
    return resolved;
  }
}
