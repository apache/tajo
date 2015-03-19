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

import io.netty.channel.ChannelHandler;

import javax.ws.rs.ProcessingException;

import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.spi.ContainerProvider;

/**
 * Container Provider for NettyRestHandlerContainer
 */
public final class NettyRestHandlerContainerProvider implements ContainerProvider {

  @Override
  public <T> T createContainer(Class<T> type, ApplicationHandler application) throws ProcessingException {
    if (type != NettyRestHandlerContainer.class && 
        (type == null || !ChannelHandler.class.isAssignableFrom(type))) {
      return null;
    }
    return type.cast(new NettyRestHandlerContainer(application));
  }

}
