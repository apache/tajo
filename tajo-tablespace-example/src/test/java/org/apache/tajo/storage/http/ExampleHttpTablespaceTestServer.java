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

package org.apache.tajo.storage.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;

public class ExampleHttpTablespaceTestServer implements Closeable {

  private final static Log LOG = LogFactory.getLog(ExampleHttpTablespaceTestServer.class);

  private ServerBootstrap bootstrap;
  private Channel channel;

  public void init() throws InterruptedException {
    EventLoopGroup group = new NioEventLoopGroup(1);

    bootstrap = new ServerBootstrap();
    bootstrap.group(group)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ExampleHttpServerInitializer());

    channel = bootstrap.bind(0).sync().channel();

    LOG.info(ExampleHttpTablespaceTestServer.class.getSimpleName() + " listening on port " + getAddress().getPort());
  }

  public InetSocketAddress getAddress() {
    return (InetSocketAddress) channel.localAddress();
  }

  public void close() {
    if (bootstrap != null) {
      if (bootstrap.group() != null) {
        bootstrap.group().shutdownGracefully();
      }
    }

    if (channel != null) {
      channel.close();
    }
  }
}
