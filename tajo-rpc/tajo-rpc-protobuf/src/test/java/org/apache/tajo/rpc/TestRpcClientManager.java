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
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestRpcClientManager {

  @Test
  public void testRaceCondition() throws Exception {
    final int parallelCount = 50;
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    ExecutorService executor = Executors.newFixedThreadPool(parallelCount);

    NettyServerBase server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), parallelCount);
    server.start();
    try {
      final InetSocketAddress address = server.getListenAddress();
      final RpcClientManager manager = RpcClientManager.getInstance();

      List<Future> tasks = new ArrayList<Future>();
      for (int i = 0; i < parallelCount; i++) {
        tasks.add(executor.submit(new Runnable() {
              @Override
              public void run() {
                NettyClientBase client = null;
                try {
                  client = manager.getClient(address, DummyProtocol.class, false);
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

      NettyClientBase clientBase = manager.getClient(address, DummyProtocol.class, false);
      RpcClientManager.cleanup(clientBase);
    } finally {
      server.shutdown();
      executor.shutdown();
      RpcClientManager.close();
    }
  }

  @Test
  public void testClientCloseEvent() throws Exception {
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    NettyServerBase server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), 3);
    server.start();
    RpcClientManager manager = RpcClientManager.getInstance();

    try {

      NettyClientBase client = manager.getClient(server.getListenAddress(), DummyProtocol.class, true);
      assertTrue(client.isConnected());
      assertTrue(client.getChannel().isWritable());

      RpcClientManager.RpcConnectionKey key = client.getKey();
      assertTrue(RpcClientManager.contains(key));

      client.close();
      assertFalse(RpcClientManager.contains(key));
    } finally {
      server.shutdown();
      RpcClientManager.close();
    }
  }

  @Test
  public void testClientCloseEventWithReconnect() throws Exception {
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    NettyServerBase server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), 3);
    server.start();
    int repeat = 10;
    RpcClientManager manager = RpcClientManager.getInstance();

    try {

      NettyClientBase client = manager.getClient(server.getListenAddress(), DummyProtocol.class, true);
      assertTrue(client.isConnected());

      RpcClientManager.RpcConnectionKey key = client.getKey();
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
      server.shutdown();
      RpcClientManager.close();
    }
  }

  @Test
  public void testUnManagedClient() throws Exception {
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    NettyServerBase server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), 3);
    server.start();
    RpcClientManager.RpcConnectionKey key =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    RpcClientManager.close();
    RpcClientManager manager = RpcClientManager.getInstance();

    try {
      NettyClientBase client1 = manager.newClient(key, 0, 0, TimeUnit.SECONDS, false);
      assertTrue(client1.isConnected());
      assertFalse(RpcClientManager.contains(key));

      NettyClientBase client2 = manager.newClient(key, 0, 0, TimeUnit.SECONDS, false);
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
    } finally {
      server.shutdown();
    }
  }
}