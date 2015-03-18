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

package org.apache.tajo.ws.rs.netty;

import org.apache.tajo.rpc.RpcEventListener;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.internal.ConfigHelper;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;

/**
 * Event subscriber for netty rest service.
 */
public class NettyRestServerListener implements RpcEventListener {
  
  private Container container;
  
  public NettyRestServerListener(Container container) {
    this.container = container;
  }

  @Override
  public void onAfterInit(Object obj) {
    
  }

  @Override
  public void onAfterShutdown(Object obj) {
    ApplicationHandler applicationHandler = new ApplicationHandler(container.getConfiguration());
    ContainerLifecycleListener lifecycleListener = ConfigHelper.getContainerLifecycleListener(applicationHandler);
    lifecycleListener.onShutdown(container);
  }

  @Override
  public void onAfterStart(Object obj) {
    ApplicationHandler applicationHandler = new ApplicationHandler(container.getConfiguration());
    ContainerLifecycleListener lifecycleListener = ConfigHelper.getContainerLifecycleListener(applicationHandler);
    lifecycleListener.onStartup(container);
  }

  @Override
  public void onBeforeInit(Object obj) {
    
  }

  @Override
  public void onBeforeShutdown(Object obj) {
    
  }

  @Override
  public void onBeforeStart(Object obj) {
    
  }

}
