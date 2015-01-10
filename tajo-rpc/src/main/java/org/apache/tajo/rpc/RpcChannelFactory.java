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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class RpcChannelFactory {
  private static final Log LOG = LogFactory.getLog(RpcChannelFactory.class);

  private static final int DEFAULT_WORKER_NUM = Runtime.getRuntime().availableProcessors() * 2;

  private static ClientSocketChannelFactory factory;
  private static AtomicInteger clientCount = new AtomicInteger(0);
  private static AtomicInteger serverCount = new AtomicInteger(0);

  private RpcChannelFactory(){
  }

  /**
   * make this factory static thus all clients can share its thread pool.
   * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
   */
  public static synchronized ClientSocketChannelFactory getSharedClientChannelFactory() {
    return getSharedClientChannelFactory(DEFAULT_WORKER_NUM);
  }

  /**
   * make this factory static thus all clients can share its thread pool.
   * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
   *
   * @param workerNum The number of workers
   */
  public static synchronized ClientSocketChannelFactory getSharedClientChannelFactory(int workerNum){
    //shared woker and boss pool
    if(factory == null){
      factory = createClientChannelFactory("Internal-Client", workerNum);
    }
    return factory;
  }

  // Client must release the external resources
  public static synchronized ClientSocketChannelFactory createClientChannelFactory(String name, int workerNum) {
    name = name + "-" + clientCount.incrementAndGet();
    if(LOG.isDebugEnabled()){
      LOG.debug("Create " + name + " ClientSocketChannelFactory. Worker:" + workerNum);
    }

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory bossFactory = builder.setNameFormat(name + " Boss #%d").build();
    ThreadFactory workerFactory = builder.setNameFormat(name + " Worker #%d").build();

    NioClientBossPool bossPool = new NioClientBossPool(Executors.newCachedThreadPool(bossFactory), 1,
        new HashedWheelTimer(), ThreadNameDeterminer.CURRENT);
    NioWorkerPool workerPool = new NioWorkerPool(Executors.newCachedThreadPool(workerFactory), workerNum,
        ThreadNameDeterminer.CURRENT);

    return new NioClientSocketChannelFactory(bossPool, workerPool);
  }

  // Client must release the external resources
  public static synchronized ServerSocketChannelFactory createServerChannelFactory(String name, int workerNum) {
    name = name + "-" + serverCount.incrementAndGet();
    if(LOG.isInfoEnabled()){
      LOG.info("Create " + name + " ServerSocketChannelFactory. Worker:" + workerNum);
    }
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory bossFactory = builder.setNameFormat(name + " Server Boss #%d").build();
    ThreadFactory workerFactory = builder.setNameFormat(name + " Server Worker #%d").build();

    NioServerBossPool bossPool =
        new NioServerBossPool(Executors.newCachedThreadPool(bossFactory), 1, ThreadNameDeterminer.CURRENT);
    NioWorkerPool workerPool =
        new NioWorkerPool(Executors.newCachedThreadPool(workerFactory), workerNum, ThreadNameDeterminer.CURRENT);

    return new NioServerSocketChannelFactory(bossPool, workerPool);
  }

  public static synchronized void shutdown(){
    if(LOG.isDebugEnabled()) {
      LOG.debug("Shutdown Shared RPC Pool");
    }
    if (factory != null) {
      factory.releaseExternalResources();
    }
    factory = null;
  }
}
