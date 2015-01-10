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

package org.apache.tajo.worker.dataserver;

import io.netty.channel.ChannelFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.worker.dataserver.retriever.DataRetriever;

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

public class HttpDataServer {
  private final static Log LOG = LogFactory.getLog(HttpDataServer.class);

  private final InetSocketAddress addr;
  private InetSocketAddress bindAddr;
  private ServerBootstrap bootstrap = null;
  private EventLoopGroup eventloopGroup = null;
  private ChannelGroup channelGroup = null;

  public HttpDataServer(final InetSocketAddress addr, 
      final DataRetriever retriever) {
    this.addr = addr;
    this.eventloopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
        Executors.defaultThreadFactory());

    // Configure the server.
    this.bootstrap = new ServerBootstrap();
    this.bootstrap.group(eventloopGroup)
      .childHandler(new HttpDataServerChannelInitializer(retriever))
      .channel(NioServerSocketChannel.class);
    this.channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  }

  public HttpDataServer(String bindaddr, DataRetriever retriever) {
    this(NetUtils.createSocketAddr(bindaddr), retriever);
  }

  public void start() {
    // Bind and start to accept incoming connections.
    ChannelFuture future = bootstrap.bind(addr).syncUninterruptibly()
            .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    channelGroup.add(future.channel());    
    this.bindAddr = (InetSocketAddress) future.channel().localAddress();
    LOG.info("HttpDataServer starts up ("
        + this.bindAddr.getAddress().getHostAddress() + ":" + this.bindAddr.getPort()
        + ")");
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }

  public void stop() {
    ChannelGroupFuture future = channelGroup.close();
    future.awaitUninterruptibly();
    eventloopGroup.shutdownGracefully();

    LOG.info("HttpDataServer shutdown ("
        + this.bindAddr.getAddress().getHostAddress() + ":"
        + this.bindAddr.getPort() + ")");
  }
}
