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

import org.apache.tajo.rpc.test.DummyProtocol;
import org.apache.tajo.rpc.test.impl.DummyProtocolAsyncImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class TestRpcClientManager {

  static NettyServerBase server;

  @BeforeClass
  public static void setupClass() throws Exception {
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), 10);
    server.start();

  }

  @AfterClass
  public static void afterClass(){
    server.shutdown(true);
  }

  @Test
  public void testRaceCondition() throws Exception {
    final int parallelCount = 50;
    ExecutorService executor = Executors.newFixedThreadPool(parallelCount);

    try {
      final InetSocketAddress address = server.getListenAddress();
      final RpcClientManager manager = RpcClientManager.getInstance();

      List<Future> tasks = new ArrayList<>();
      for (int i = 0; i < parallelCount; i++) {
        tasks.add(executor.submit(new Runnable() {
              @Override
              public void run() {
                NettyClientBase client = null;
                try {
                  client = manager.getClient(address, DummyProtocol.class, true, new Properties());
                } catch (Throwable e) {
                  fail(e.getMessage());
                }
                assertTrue(client.isConnected());
              }
            })
        );
      }

      for (Future future : tasks) {
        future.get();
      }

      NettyClientBase clientBase = manager.getClient(address, DummyProtocol.class, true, new Properties());
      RpcClientManager.cleanup(clientBase);
    } finally {
      executor.shutdown();
      RpcClientManager.close();
    }
  }

  @Test
  public void testClientCloseEvent() throws Exception {
    RpcClientManager manager = RpcClientManager.getInstance();

    try {

      NettyClientBase client = manager.getClient(server.getListenAddress(), DummyProtocol.class, true, new Properties());
      assertTrue(client.isConnected());
      assertTrue(client.getChannel().isWritable());

      RpcConnectionKey key = client.getKey();
      assertTrue(RpcClientManager.contains(key));

      client.close();
      assertFalse(RpcClientManager.contains(key));
    } finally {
      RpcClientManager.close();
    }
  }

  @Test
  public void testClientCloseEventWithReconnect() throws Exception {
    int repeat = 100;
    RpcClientManager manager = RpcClientManager.getInstance();

    try {

      NettyClientBase client = manager.getClient(server.getListenAddress(), DummyProtocol.class, true, new Properties());
      assertTrue(client.isConnected());

      RpcConnectionKey key = client.getKey();
      assertTrue(RpcClientManager.contains(key));

      client.close();
      assertFalse(client.isConnected());
      assertFalse(RpcClientManager.contains(key));

      for (int i = 0; i < repeat; i++) {
        client.connect();
        assertTrue(client.isConnected());
        assertTrue(RpcClientManager.contains(key));

        client.close();
        assertFalse(client.isConnected());
        assertFalse(RpcClientManager.contains(key));
      }
    } finally {
      RpcClientManager.close();
    }
  }

  @Test
  public void testUnManagedClient() throws Exception {
    RpcConnectionKey key =
        new RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    RpcClientManager.close();
    RpcClientManager manager = RpcClientManager.getInstance();

    NettyClientBase client1 = manager.newClient(key, new Properties());
    assertTrue(client1.isConnected());
    assertFalse(RpcClientManager.contains(key));

    NettyClientBase client2 = manager.newClient(key, new Properties());
    assertTrue(client2.isConnected());
    assertFalse(RpcClientManager.contains(key));

    assertEquals(client1.getRemoteAddress(), client2.getRemoteAddress());
    assertNotEquals(client1.getChannel(), client2.getChannel());

    client1.close();
    assertFalse(client1.isConnected());
    assertTrue(client2.isConnected());

    client1.connect();
    client2.close();
    assertFalse(client2.isConnected());
    assertTrue(client1.isConnected());

    RpcClientManager.cleanup(client1, client2);
  }
}