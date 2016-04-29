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

import java.net.*;

public class NetUtils {

  /**
   * Compose a "host:port" string from the address.
   */
  public static String getHostPortString(InetSocketAddress addr) {
    return getHostPortString(addr.getHostName(), addr.getPort());
  }

  public static String getHostPortString(String host, int port) {
    return host + ":" + port;
  }

  public static String normalizeInetSocketAddress(InetSocketAddress addr) {
    return getHostPortString(addr.getAddress().getHostAddress(), addr.getPort());
  }

  public static InetSocketAddress createSocketAddr(String addr) {
    String [] splitted = addr.split(":");
    return new InetSocketAddress(splitted[0], Integer.parseInt(splitted[1]));
  }

  /**
   * Util method to build socket addr from either:
   *   <host>
   *   <host>:<port>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String host, int port) {
    return new InetSocketAddress(host, port);
  }

  public static InetSocketAddress createUnresolved(String addr) {
    String [] splitted = addr.split(":");
    return InetSocketAddress.createUnresolved(splitted[0], Integer.parseInt(splitted[1]));
  }

  /**
   * Returns InetSocketAddress that a client can use to
   * connect to the server. NettyServerBase.getListenerAddress() is not correct when
   * the server binds to "0.0.0.0". This returns "hostname:port" of the server,
   * or "127.0.0.1:port" when the getListenerAddress() returns "0.0.0.0:port".
   *
   * @param addr of a listener
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(InetSocketAddress addr) {
    if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress()) {
      try {
        addr = new InetSocketAddress(InetAddress.getLocalHost(), addr.getPort());
      } catch (UnknownHostException uhe) {
        // shouldn't get here unless the host doesn't have a loopback iface
        addr = new InetSocketAddress("127.0.0.1", addr.getPort());
      }
    }
    InetSocketAddress canonicalAddress =
        new InetSocketAddress(addr.getAddress().getCanonicalHostName(), addr.getPort());
    return canonicalAddress;
  }

  /**
   * Given an InetAddress, checks to see if the address is a local address, by
   * comparing the address with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static boolean isLocalAddress(InetAddress addr) {
    // Check if the address is any local or loop back
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
        local = false;
      }
    }
    return local;
  }

  public static String normalizeHost(String host) {
    try {
      InetAddress address = InetAddress.getByName(host);
      if (isLocalAddress(address)) {
        return InetAddress.getLocalHost().getHostAddress();
      } else {
        return address.getHostAddress();
      }
    } catch (UnknownHostException e) {
    }
    return host;
  }
}