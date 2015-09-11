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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NettyUtils {
  private static final Log LOG = LogFactory.getLog(NettyUtils.class);
  
  private static final int DEFAULT_THREAD_NUM = Runtime.getRuntime().availableProcessors() * 2;

  private static final Object lockObjectForLoopGroup = new Object();
  private static AtomicInteger serverCount = new AtomicInteger(0);

  public enum GROUP {
    DEFAULT,
    FETCHER
  }

  private static final Map<GROUP, EventLoopGroup> eventLoopGroupMap =
      new ConcurrentHashMap<GROUP, EventLoopGroup>();

  private NettyUtils(){
  }

  /**
   * Get default EventLoopGroup of netty’s. servers and clients can shared it.
   */
  public static EventLoopGroup getDefaultEventLoopGroup() {
    return getSharedEventLoopGroup(GROUP.DEFAULT, DEFAULT_THREAD_NUM);
  }

  /**
   * Get EventLoopGroup of netty’s.
   *
   * @param clientId
   * @param threads
   * @return A EventLoopGroup by key
   */
  public static EventLoopGroup getSharedEventLoopGroup(GROUP clientId, int threads) {
    EventLoopGroup returnEventLoopGroup;

    synchronized (lockObjectForLoopGroup) {
      if (!eventLoopGroupMap.containsKey(clientId)) {
        eventLoopGroupMap.put(clientId, createEventLoopGroup(clientId.name(), threads));
      }

      returnEventLoopGroup = eventLoopGroupMap.get(clientId);
      if (isEventLoopGroupShuttingDown(returnEventLoopGroup)) {
        returnEventLoopGroup = createEventLoopGroup(clientId.name(), threads);
        eventLoopGroupMap.put(clientId, returnEventLoopGroup);
      }
    }

    return returnEventLoopGroup;
  }

  public static EventLoopGroup createEventLoopGroup(String name, int threads) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Create " + name + " EventLoopGroup. threads:" + threads);
    }

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory clientFactory = builder.setNameFormat(name + " #%d").build();

    return createEventLoopGroup(threads, clientFactory);
  }

  protected static boolean isEventLoopGroupShuttingDown(EventLoopGroup eventLoopGroup) {
    return ((eventLoopGroup == null) || eventLoopGroup.isShuttingDown());
  }

  private static EventLoopGroup createEventLoopGroup(int threads, ThreadFactory factory) {
    return new NioEventLoopGroup(threads, factory);
  }

  /**
   * Server must release the external resources
   */
  public static ServerBootstrap createServerBootstrap(String name, int threads) {
    name = name + "-" + serverCount.incrementAndGet();

    EventLoopGroup eventLoopGroup = createEventLoopGroup(name, threads);
    return new ServerBootstrap().group(eventLoopGroup, eventLoopGroup);
  }

  public static void shutdownGracefully() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Shutdown Shared RPC Pool");
    }
    synchronized (lockObjectForLoopGroup) {
      for (EventLoopGroup eventLoopGroup : eventLoopGroupMap.values()) {
        try {
          shutdown(eventLoopGroup).sync();
        } catch (InterruptedException e) {
          //ignore
        }
      }
      eventLoopGroupMap.clear();
    }
  }

  public static io.netty.util.concurrent.Future shutdown(EventLoopGroup eventLoopGroup) {
    if (eventLoopGroup != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Shutdown EventLoopGroup :" + eventLoopGroup.toString());
      }

      return eventLoopGroup.shutdownGracefully();
    }
    return null;
  }
}
