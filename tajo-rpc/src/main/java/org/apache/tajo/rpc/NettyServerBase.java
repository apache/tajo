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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyServerBase {
  private static final Log LOG = LogFactory.getLog(NettyServerBase.class);
  private static final String DEFAULT_PREFIX = "RpcServer_";
  private static final AtomicInteger sequenceId = new AtomicInteger(0);

  protected String serviceName;
  protected InetSocketAddress serverAddr;
  protected InetSocketAddress bindAddress;
  protected ChannelPipelineFactory pipelineFactory;
  protected ServerBootstrap bootstrap;
  protected Channel channel;
  protected ChannelGroup accepted = new DefaultChannelGroup();

  private InetSocketAddress initIsa;

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

  public void init(ChannelPipelineFactory pipeline, int workerNum) {
    ChannelFactory factory = RpcChannelFactory.createServerChannelFactory(serviceName, workerNum);

    pipelineFactory = pipeline;
    bootstrap = new ServerBootstrap(factory);
    bootstrap.setPipelineFactory(pipelineFactory);
    // TODO - should be configurable
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.connectTimeoutMillis", 10000);
    bootstrap.setOption("child.connectResponseTimeoutMillis", 10000);
    bootstrap.setOption("child.receiveBufferSize", 1048576 * 10);
  }

  public InetSocketAddress getListenAddress() {
    return this.bindAddress;
  }

  public void start() {
    if (serviceName == null) {
      this.serviceName = getNextDefaultServiceName();
    }

    if (initIsa.getPort() == 0) {
      try {
        int port = getUnusedPort();
        serverAddr = new InetSocketAddress(initIsa.getHostName(), port);
      } catch (IOException e) {
        LOG.error(e);
      }
    } else {
      serverAddr = initIsa;
    }

    this.channel = bootstrap.bind(serverAddr);
    this.bindAddress = (InetSocketAddress) channel.getLocalAddress();

    LOG.info("Rpc (" + serviceName + ") listens on " + this.bindAddress);
  }

  public Channel getChannel() {
    return this.channel;
  }

  public void shutdown() {
    if(channel != null) {
      channel.close().awaitUninterruptibly();
    }

    try {
      accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    if(bootstrap != null) {
      bootstrap.releaseExternalResources();
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


  private synchronized static int getUnusedPort() throws IOException {
    while (true) {
      int port = nextPortNum.getAndIncrement();
      if (port >= endPortRange) {
        synchronized (nextPortNum) {
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
}