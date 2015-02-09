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

package org.apache.tajo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class HttpFileServer {
  private final static Log LOG = LogFactory.getLog(HttpFileServer.class);

  private final InetSocketAddress addr;
  private InetSocketAddress bindAddr;
  private ServerBootstrap bootstrap = null;
  private EventLoopGroup eventloopGroup = null;
  private ChannelGroup channelGroup = null;

  public HttpFileServer(final InetSocketAddress addr) {
    this.addr = addr;
    this.eventloopGroup = new NioEventLoopGroup(2, Executors.defaultThreadFactory());

    // Configure the server.
    this.bootstrap = new ServerBootstrap();
    this.bootstrap.childHandler(new HttpFileServerChannelInitializer())
          .group(eventloopGroup)
          .channel(NioServerSocketChannel.class);
    this.channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  }

  public HttpFileServer(String bindaddr) {
    this(NetUtils.createSocketAddr(bindaddr));
  }

  public void start() {
    // Bind and start to accept incoming connections.
    ChannelFuture future = bootstrap.bind(addr).syncUninterruptibly();
    channelGroup.add(future.channel());    
    this.bindAddr = (InetSocketAddress) future.channel().localAddress();
    LOG.info("HttpFileServer starts up ("
        + this.bindAddr.getAddress().getHostAddress() + ":" + this.bindAddr.getPort()
        + ")");
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }

  public void stop() {
    channelGroup.close();
    eventloopGroup.shutdownGracefully();

    LOG.info("HttpFileServer shutdown ("
        + this.bindAddr.getAddress().getHostAddress() + ":"
        + this.bindAddr.getPort() + ")");
  }
}
