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
import io.netty.channel.ChannelInboundHandler;
import org.apache.tajo.ws.rs.netty.testapp1.TestApplication1;
import org.glassfish.jersey.server.ApplicationHandler;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class NettyRestHandlerContainerProviderTest {

  private NettyRestHandlerContainerProvider provider;
  private ApplicationHandler applicationHandler;

  @Before
  public void setUp() throws Exception {
    provider = new NettyRestHandlerContainerProvider();
    applicationHandler = new ApplicationHandler(new TestApplication1());
  }

  @Test
  public void testCreation() throws Exception {
    ChannelHandler handler = provider.createContainer(ChannelHandler.class, applicationHandler);

    assertNotNull(handler);

    ChannelInboundHandler inboundHandler = provider.createContainer(ChannelInboundHandler.class, applicationHandler);

    assertNotNull(inboundHandler);

    NettyRestHandlerContainer container = provider.createContainer(NettyRestHandlerContainer.class, applicationHandler);

    assertNotNull(container);
  }

  @Test
  public void testNullCreation() throws Exception {
    String stringValue = provider.createContainer(String.class, applicationHandler);

    assertNull(stringValue);

    Object objectValue = provider.createContainer(Object.class, applicationHandler);

    assertNull(objectValue);
  }
}
