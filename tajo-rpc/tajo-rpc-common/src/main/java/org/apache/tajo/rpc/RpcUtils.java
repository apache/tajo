/*
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

package org.apache.tajo.rpc;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;

public class RpcUtils {

  public static String normalizeInetSocketAddress(InetSocketAddress addr) {
    return addr.getAddress().getHostAddress() + ":" + addr.getPort();
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

  public static InetSocketAddress createUnresolved(String addr) {
    String [] splitted = addr.split(":");
    return InetSocketAddress.createUnresolved(splitted[0], Integer.parseInt(splitted[1]));
  }

  // non-blocking lock which passes only a ticket before cleared or removed
  public static class Scrutineer<T> {

    private final AtomicReference<T> reference = new AtomicReference<T>();

    public T expire() {
      return reference.getAndSet(null);
    }

    public T check(T ticket) {
      T granted = reference.get();
      for (;granted == null; granted = reference.get()) {
        if (reference.compareAndSet(null, ticket)) {
          return ticket;
        }
      }
      return granted;
    }

    public boolean clear(T granted) {
      return reference.compareAndSet(granted, null);
    }
  }
}
