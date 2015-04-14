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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRpcConnectionManager {

  @Test
  public void testRaceCondition() throws Exception {
    final int parallelCount = 50;
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    NettyServerBase server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), parallelCount);
    server.start();

    final InetSocketAddress address = server.getListenAddress();
    final RpcConnectionManager manager = RpcConnectionManager.getInstance();

    ExecutorService executor = Executors.newFixedThreadPool(parallelCount);
    List<Future> tasks = new ArrayList<Future>();
    for (int i = 0; i < parallelCount; i++) {
      tasks.add(executor.submit(new Runnable() {
            @Override
            public void run() {
              NettyClientBase clientBase = null;
              try {
                clientBase = manager.getConnection(address, DummyProtocol.class, true);
              } catch (Throwable e) {
                fail(e.getMessage());
              }
              assertTrue(clientBase.isConnected());
            }
          })
      );

      for (Future future : tasks) {
        future.get();
      }
    }

    NettyClientBase clientBase = manager.getConnection(address, DummyProtocol.class, true);
    manager.cleanup(clientBase);
    server.shutdown();
  }

  @Test
  public void testCloseFuture() throws Exception {
    final int parallelCount = 50;
    final DummyProtocolAsyncImpl service = new DummyProtocolAsyncImpl();
    NettyServerBase server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), parallelCount);
    server.start();

    final InetSocketAddress address = server.getListenAddress();
    final RpcConnectionManager manager = RpcConnectionManager.getInstance();

    NettyClientBase clientBase = manager.getConnection(address, DummyProtocol.class, true);
    assertTrue(clientBase.isConnected());
    assertTrue(clientBase.getChannel().isWritable());

    RpcConnectionManager.RpcConnectionKey key = clientBase.getKey();
    assertTrue(manager.contains(key));

    clientBase.close();
    assertFalse(manager.contains(key));
    server.shutdown();
  }
}