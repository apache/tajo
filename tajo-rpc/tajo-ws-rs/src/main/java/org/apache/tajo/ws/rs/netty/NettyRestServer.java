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

import io.netty.channel.ChannelHandler;
import java.net.InetSocketAddress;

import org.apache.tajo.rpc.PublicServiceProvider;
import org.apache.tajo.rpc.NettyServerBase;

/**
 * JAX-RS Http Server on Netty implementation.
 */
public class NettyRestServer extends NettyServerBase implements PublicServiceProvider {
  
  private ChannelHandler handler;
  private int workerCount;

  public NettyRestServer(InetSocketAddress address, int workerCount) {
    this("NettyRestService", address, workerCount);
  }

  public NettyRestServer(String serviceName, InetSocketAddress address, int workerCount) {
    super(serviceName, address);

    this.workerCount = workerCount;
  }

  public ChannelHandler getHandler() {
    return handler;
  }

  public void setHandler(ChannelHandler handler) {
    this.handler = handler;
  }

  /**
   * Bind desired port and start network service. Before starting network service, {@link NettyRestServer}
   * will initialize its configuration.
   * 
   */
  @Override
  public void start() {
    if (handler == null) {
      throw new IllegalStateException("ChannelHandler is null.");
    }
    
    super.init(new NettyRestChannelInitializer(handler), workerCount);
    super.start();
  }

}
