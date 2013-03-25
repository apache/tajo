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

package tajo.worker.dataserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import tajo.worker.dataserver.retriever.DataRetriever;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class HttpDataServer {
  private final static Log LOG = LogFactory.getLog(HttpDataServer.class);

  private final InetSocketAddress addr;
  private InetSocketAddress bindAddr;
  private ServerBootstrap bootstrap = null;
  private ChannelFactory factory = null;
  private ChannelGroup channelGroup = null;

  public HttpDataServer(final InetSocketAddress addr, 
      final DataRetriever retriever) {
    this.addr = addr;
    this.factory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
        Runtime.getRuntime().availableProcessors() * 2);

    // Configure the server.
    this.bootstrap = new ServerBootstrap(factory);
    // Set up the event pipeline factory.
    this.bootstrap.setPipelineFactory(
        new HttpDataServerPipelineFactory(retriever));    
    this.channelGroup = new DefaultChannelGroup();
  }

  public HttpDataServer(String bindaddr, DataRetriever retriever) {
    this(NetUtils.createSocketAddr(bindaddr), retriever);
  }

  public void start() {
    // Bind and start to accept incoming connections.
    Channel channel = bootstrap.bind(addr);
    channelGroup.add(channel);    
    this.bindAddr = (InetSocketAddress) channel.getLocalAddress();
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
    factory.releaseExternalResources();

    LOG.info("HttpDataServer shutdown ("
        + this.bindAddr.getAddress().getHostAddress() + ":"
        + this.bindAddr.getPort() + ")");
  }
}
