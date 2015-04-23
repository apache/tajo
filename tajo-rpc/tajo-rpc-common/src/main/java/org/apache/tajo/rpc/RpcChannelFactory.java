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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class RpcChannelFactory {
  private static final Log LOG = LogFactory.getLog(RpcChannelFactory.class);
  
  private static final int DEFAULT_WORKER_NUM = Runtime.getRuntime().availableProcessors() * 2;

  private static final Object lockObjectForLoopGroup = new Object();
  private static AtomicInteger serverCount = new AtomicInteger(0);

  public enum ClientChannelId {
    CLIENT_DEFAULT,
    FETCHER
  }

  private static final Map<ClientChannelId, Integer> defaultMaxKeyPoolCount =
      new ConcurrentHashMap<ClientChannelId, Integer>();
  private static final Map<ClientChannelId, Queue<EventLoopGroup>> eventLoopGroupPool =
      new ConcurrentHashMap<ClientChannelId, Queue<EventLoopGroup>>();

  private RpcChannelFactory(){
  }
  
  static {
    Runtime.getRuntime().addShutdownHook(new CleanUpHandler());

    defaultMaxKeyPoolCount.put(ClientChannelId.CLIENT_DEFAULT, 1);
    defaultMaxKeyPoolCount.put(ClientChannelId.FETCHER, 1);
  }

  /**
  * make this factory static thus all clients can share its thread pool.
  * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
  */
  public static EventLoopGroup getSharedClientEventloopGroup() {
    return getSharedClientEventloopGroup(DEFAULT_WORKER_NUM);
  }
  
  /**
  * make this factory static thus all clients can share its thread pool.
  * NioClientSocketChannelFactory has only one method newChannel() visible for user, which is thread-safe
  *
  * @param workerNum The number of workers
  */
  public static EventLoopGroup getSharedClientEventloopGroup(int workerNum){
    //shared woker and boss pool
    return getSharedClientEventloopGroup(ClientChannelId.CLIENT_DEFAULT, workerNum);
  }

  /**
   * This function return eventloopgroup by key. Fetcher client will have one or more eventloopgroup for its throughput.
   *
   * @param clientId
   * @param workerNum
   * @return
   */
  public static EventLoopGroup getSharedClientEventloopGroup(ClientChannelId clientId, int workerNum) {
    Queue<EventLoopGroup> eventLoopGroupQueue;
    EventLoopGroup returnEventLoopGroup;

    synchronized (lockObjectForLoopGroup) {
      eventLoopGroupQueue = eventLoopGroupPool.get(clientId);
      if (eventLoopGroupQueue == null) {
        eventLoopGroupQueue = createClientEventloopGroups(clientId, workerNum);
      }

      returnEventLoopGroup = eventLoopGroupQueue.poll();
      if (isEventLoopGroupShuttingDown(returnEventLoopGroup)) {
        returnEventLoopGroup = createClientEventloopGroup(clientId.name(), workerNum);
      }
      eventLoopGroupQueue.add(returnEventLoopGroup);
    }

    return returnEventLoopGroup;
  }

  protected static boolean isEventLoopGroupShuttingDown(EventLoopGroup eventLoopGroup) {
    return ((eventLoopGroup == null) || eventLoopGroup.isShuttingDown());
  }

  // Client must release the external resources
  protected static Queue<EventLoopGroup> createClientEventloopGroups(ClientChannelId clientId, int workerNum) {
    int defaultMaxObjectCount = defaultMaxKeyPoolCount.get(clientId);
    Queue<EventLoopGroup> loopGroupQueue = new ConcurrentLinkedQueue<EventLoopGroup>();
    eventLoopGroupPool.put(clientId, loopGroupQueue);

    for (int objectIdx = 0; objectIdx < defaultMaxObjectCount; objectIdx++) {
      loopGroupQueue.add(createClientEventloopGroup(clientId.name(), workerNum));
    }

    return loopGroupQueue;
  }

  protected static EventLoopGroup createClientEventloopGroup(String name, int workerNum) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Create " + name + " ClientEventLoopGroup. Worker:" + workerNum);
    }

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory clientFactory = builder.setNameFormat(name + " Client #%d").build();

    return new NioEventLoopGroup(workerNum, clientFactory);
  }

  // Client must release the external resources
  public static ServerBootstrap createServerChannelFactory(String name, int workerNum) {
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

    synchronized(lockObjectForLoopGroup) {
      for (Queue<EventLoopGroup> eventLoopGroupQueue: eventLoopGroupPool.values()) {
        for (EventLoopGroup eventLoopGroup: eventLoopGroupQueue) {
          eventLoopGroup.shutdownGracefully();
        }

        eventLoopGroupQueue.clear();
      }
      eventLoopGroupPool.clear();
    }
  }
  
  static class CleanUpHandler extends Thread {

    @Override
    public void run() {
      RpcChannelFactory.shutdownGracefully();
    }
    
  }
}
