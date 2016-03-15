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

package org.apache.tajo.ws.rs.netty;

import java.net.InetSocketAddress;
import java.net.URI;

import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Factory class for creating {@link NettyRestServer} instances
 */
public final class NettyRestServerFactory {

  public static NettyRestServer createNettyRestServer(URI uri, ResourceConfig configuration, int workerCount) {
    return createNettyRestServer(uri, new NettyRestHandlerContainer(configuration), workerCount, true);
  }

  public static NettyRestServer createNettyRestServer(URI uri, ResourceConfig configuration, int workerCount,
      boolean start) {
    return createNettyRestServer(uri, new NettyRestHandlerContainer(configuration), workerCount, start);
  }

  public static NettyRestServer createNettyRestServer(URI uri, ResourceConfig configuration,
      ServiceLocator parentLocator, int workerCount) {
    return createNettyRestServer(uri, new NettyRestHandlerContainer(configuration, parentLocator), workerCount, true);
  }

  public static NettyRestServer createNettyRestServer(URI uri, ResourceConfig configuration,
      ServiceLocator parentLocator, int workerCount, boolean start) {
    return createNettyRestServer(uri, new NettyRestHandlerContainer(configuration, parentLocator), workerCount, start);
  }

  /**
   * Creates {@link NettyRestServer} instances with JAX-RS application.
   * 
   * @param uri
   * @param handler
   * @param start
   * @return
   */
  private static NettyRestServer createNettyRestServer(URI uri, NettyRestHandlerContainer handler, int workerCount,
      boolean start) {
    if (uri == null) {
      throw new IllegalArgumentException("uri is null.");
    }

    String schemeString = uri.getScheme();
    if (!schemeString.equalsIgnoreCase("http") && !schemeString.equalsIgnoreCase("https")) {
      throw new IllegalArgumentException("scheme of this uri (" + uri.toString() + ") should be http or https.");
    }

    int port = uri.getPort();
    if (port == -1) {
      throw new IllegalArgumentException("Port number should be provided.");
    }

    handler.setRootPath(uri.getPath());

    InetSocketAddress bindAddress = new InetSocketAddress(uri.getHost(), port);
    NettyRestServer nettyRestServer = new NettyRestServer("Tajo-REST", bindAddress, workerCount);

    nettyRestServer.setHandler(handler);
    nettyRestServer.addListener(new NettyRestServerListener(handler));

    if (start) {
      nettyRestServer.start();
    }

    return nettyRestServer;
  }
}
