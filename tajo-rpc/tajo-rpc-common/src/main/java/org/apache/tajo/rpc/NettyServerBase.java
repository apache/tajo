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

package org.apache.tajo.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for netty implementation.
 */
public class NettyServerBase {
  private static final Log LOG = LogFactory.getLog(NettyServerBase.class);
  private static final String DEFAULT_PREFIX = "RpcServer_";
  private static final AtomicInteger sequenceId = new AtomicInteger(0);

  protected String serviceName;
  protected InetSocketAddress serverAddr;
  protected InetSocketAddress bindAddress;
  protected ChannelInitializer<Channel> initializer;
  protected ServerBootstrap bootstrap;
  protected ChannelFuture channelFuture;
  protected ChannelGroup accepted = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private InetSocketAddress initIsa;
  private Set<RpcEventListener> listeners = Collections.synchronizedSet(new HashSet<RpcEventListener>());

  public NettyServerBase(InetSocketAddress address) {
    this.initIsa = address;
  }

  public NettyServerBase(String serviceName, InetSocketAddress addr) {
    this.serviceName = serviceName;
    this.initIsa = addr;
  }

  public void setName(String name) {
    this.serviceName = name;
  }

  public void init(ChannelInitializer<Channel> initializer, int workerNum) {
    for (RpcEventListener listener: listeners) {
      listener.onBeforeInit(this);
    }
    
    bootstrap = NettyUtils.createServerBootstrap(serviceName, workerNum);

    this.initializer = initializer;
    bootstrap
      .channel(NioServerSocketChannel.class)
      .childHandler(initializer)
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.TCP_NODELAY, true)
      .childOption(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
      .childOption(ChannelOption.TCP_NODELAY, true)
      .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
      .childOption(ChannelOption.SO_RCVBUF, 1048576 * 10);
    
    for (RpcEventListener listener: listeners) {
      listener.onAfterInit(this);
    }
  }

  public InetSocketAddress getListenAddress() {
    return this.bindAddress;
  }

  public void start() {
    for (RpcEventListener listener: listeners) {
      listener.onBeforeStart(this);
    }
    
    if (serviceName == null) {
      this.serviceName = getNextDefaultServiceName();
    }

    if (initIsa.getPort() == 0) {
      try {
        int port = getUnusedPort();
        serverAddr = new InetSocketAddress(initIsa.getHostName(), port);
      } catch (IOException e) {
        LOG.error(e, e);
      }
    } else {
      serverAddr = initIsa;
    }

    this.channelFuture = bootstrap.clone().bind(serverAddr).syncUninterruptibly();
    this.bindAddress = (InetSocketAddress) channelFuture.channel().localAddress();

    for (RpcEventListener listener: listeners) {
      listener.onAfterStart(this);
    }
    LOG.info("Rpc (" + serviceName + ") listens on " + this.bindAddress);
  }

  public Channel getChannel() {
    return this.channelFuture.channel();
  }

  public void shutdown() {
    shutdown(false);
  }

  public void shutdown(boolean waitUntilThreadsStop) {
    for (RpcEventListener listener: listeners) {
      listener.onBeforeShutdown(this);
    }

    try {
      accepted.close();

      if (bootstrap != null) {
        if (bootstrap.childGroup() != null) {
          Future future = bootstrap.childGroup().shutdownGracefully();
          if (waitUntilThreadsStop) {
            future.sync();
          }
        }

        if (bootstrap.group() != null) {
          Future future = bootstrap.group().shutdownGracefully();
          if (waitUntilThreadsStop) {
            future.sync();
          }
        }
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    
    for (RpcEventListener listener: listeners) {
      listener.onAfterShutdown(this);
    }

    if (bindAddress != null) {
      LOG.info("Rpc (" + serviceName + ") listened on "
          + RpcUtils.normalizeInetSocketAddress(bindAddress)+ ") shutdown");
    }
  }

  private static String getNextDefaultServiceName() {
    return DEFAULT_PREFIX + sequenceId.getAndIncrement();
  }

  private static final int startPortRange = 10000;
  private static final int endPortRange = 50000;
  private static final Random rnd = new Random(System.currentTimeMillis());
  // each system has a different starting port number within the given range.
  private static final AtomicInteger nextPortNum =
      new AtomicInteger(startPortRange+ rnd.nextInt(endPortRange - startPortRange));
  private static final Object lockObject = new Object();


  private synchronized static int getUnusedPort() throws IOException {
    while (true) {
      int port = nextPortNum.getAndIncrement();
      if (port >= endPortRange) {
        synchronized (lockObject) {
          nextPortNum.set(startPortRange);
          port = nextPortNum.getAndIncrement();
        }
      }
      if (available(port)) {
        return port;
      }
    }
  }

  private static boolean available(int port) throws IOException {
    if (port < 1024 || port > 65535) {
      throw new IllegalArgumentException("Port Number Out of Bound: " + port);
    }

    ServerSocket ss = null;
    DatagramSocket ds = null;

    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);

      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);

      return true;

    } catch (IOException e) {
      return false;
    } finally {
      if (ss != null) {
        ss.close();
      }

      if (ds != null) {
        ds.close();
      }
    }
  }
  
  public void addListener(RpcEventListener listener) {
    listeners.add(listener);
  }
  
  public void removeListener(RpcEventListener listener) {
    listeners.remove(listener);
  }
}