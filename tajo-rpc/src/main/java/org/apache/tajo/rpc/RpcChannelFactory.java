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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class RpcChannelFactory {
  private static final Log LOG = LogFactory.getLog(RpcChannelFactory.class);
  
  private static final int DEFAULT_WORKER_NUM = Runtime.getRuntime().availableProcessors() * 2;
  
  private static EventLoopGroup loopGroup;
  private static final Object lockObjectForLoopGroup = new Object();
  private static AtomicInteger clientCount = new AtomicInteger(0);
  private static AtomicInteger serverCount = new AtomicInteger(0);

  private RpcChannelFactory(){
  }

  /**
  * make this factory static thus all clients can share its thread pool.
  * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
  */
  public static synchronized EventLoopGroup getSharedClientEventloopGroup() {
    return getSharedClientEventloopGroup(DEFAULT_WORKER_NUM);
  }
  
  /**
  * make this factory static thus all clients can share its thread pool.
  * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
  *
  * @param workerNum The number of workers
  */
  public static synchronized EventLoopGroup getSharedClientEventloopGroup(int workerNum){
    //shared woker and boss pool
    if (loopGroup == null) {
      synchronized (lockObjectForLoopGroup) {
        if (loopGroup == null) {
          loopGroup = createClientEventloopGroup("Internal-Client", workerNum);
        }
      }
    }
    
    return loopGroup;
  }

  // Client must release the external resources
  protected static synchronized EventLoopGroup createClientEventloopGroup(String name, int workerNum) {
    name = name + "-" + clientCount.incrementAndGet();
    if(LOG.isDebugEnabled()){
      LOG.debug("Create " + name + " ClientEventLoopGroup. Worker:" + workerNum);
    }

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory clientFactory = builder.setNameFormat(name + " Client #%d").build();

    return new NioEventLoopGroup(workerNum, clientFactory);
  }

  // Client must release the external resources
  public static synchronized ServerBootstrap createServerChannelFactory(String name, int workerNum) {
    name = name + "-" + serverCount.incrementAndGet();
    if(LOG.isInfoEnabled()){
      LOG.info("Create " + name + " ServerSocketChannelFactory. Worker:" + workerNum);
    }
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory bossFactory = builder.setNameFormat(name + " Server Boss #%d").build();
    ThreadFactory workerFactory = builder.setNameFormat(name + " Server Worker #%d").build();
    
    EventLoopGroup bossGroup =
        new NioEventLoopGroup(1, bossFactory);
    EventLoopGroup workerGroup = 
        new NioEventLoopGroup(workerNum, workerFactory);
    
    return new ServerBootstrap().group(bossGroup, workerGroup);
  }

  public static void shutdownGracefully(){
    if(LOG.isDebugEnabled()) {
      LOG.debug("Shutdown Shared RPC Pool");
    }
    
    if (loopGroup != null) {
      synchronized(lockObjectForLoopGroup) {
        if (loopGroup != null) {
          loopGroup.shutdownGracefully();
          loopGroup.terminationFuture().awaitUninterruptibly(10, TimeUnit.SECONDS);
          loopGroup = null;
        }
      }
    }
  }
}
