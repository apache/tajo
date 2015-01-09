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
import org.apache.tajo.rpc.test.DummyProtocol.DummyProtocolService.BlockingInterface;
import org.apache.tajo.rpc.test.TestProtos.EchoMessage;
import org.apache.tajo.rpc.test.TestProtos.SumRequest;
import org.apache.tajo.rpc.test.TestProtos.SumResponse;
import org.apache.tajo.rpc.test.impl.DummyProtocolBlockingImpl;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestBlockingRpc {
  public static final String MESSAGE = "TestBlockingRpc";

  private BlockingRpcServer server;
  private BlockingRpcClient client;
  private BlockingInterface stub;
  private DummyProtocolBlockingImpl service;
  private int retries;
  private ClientSocketChannelFactory clientChannelFactory;

  @Before
  public void setUp() throws Exception {
    retries = 1;

    clientChannelFactory = RpcChannelFactory.createClientChannelFactory(MESSAGE, 2);

    service = new DummyProtocolBlockingImpl();
    server = new BlockingRpcServer(DummyProtocol.class, service,
        new InetSocketAddress("127.0.0.1", 0), 2);
    server.start();
    client = new BlockingRpcClient(DummyProtocol.class,
        RpcUtils.getConnectAddress(server.getListenAddress()), clientChannelFactory, retries);
    stub = client.getStub();
  }

  @After
  public void tearDown() throws Exception {
    if(client != null) {
      client.close();
    }

    if(server != null) {
      server.shutdown();
    }

    if(clientChannelFactory != null){
      clientChannelFactory.releaseExternalResources();
    }
  }

  @Test
  public void testRpc() throws Exception {
    SumRequest request = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();
    SumResponse response1 = stub.sum(null, request);
    assertEquals(8.15d, response1.getResult(), 1e-15);

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    EchoMessage response2 = stub.echo(null, message);
    assertEquals(MESSAGE, response2.getMessage());
  }

  @Test
  public void testRpcWithServiceCallable() throws Exception {
    RpcConnectionPool pool = RpcConnectionPool.newPool(getClass().getSimpleName(), 2);
    final SumRequest request = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();

    SumResponse response =
    new ServerCallable<SumResponse>(pool,
        server.getListenAddress(), DummyProtocol.class, false) {
      @Override
      public SumResponse call(NettyClientBase client) throws Exception {
        BlockingInterface stub2 = client.getStub();
        SumResponse response1 = stub2.sum(null, request);
        return response1;
      }
    }.withRetries();

    assertEquals(8.15d, response.getResult(), 1e-15);

    response =
        new ServerCallable<SumResponse>(pool,
            server.getListenAddress(), DummyProtocol.class, false) {
          @Override
          public SumResponse call(NettyClientBase client) throws Exception {
            BlockingInterface stub2 = client.getStub();
            SumResponse response1 = stub2.sum(null, request);
            return response1;
          }
        }.withoutRetries();

    assertTrue(8.15d == response.getResult());
    pool.close();
  }

  @Test
  public void testThrowException() throws Exception {
    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    try {
      stub.throwException(null, message);
      fail("RpcCall should throw exception");
    } catch (Throwable t) {
      assertTrue(t instanceof TajoServiceException);
      assertEquals("Exception Test", t.getMessage());
      TajoServiceException te = (TajoServiceException)t;
      assertEquals("org.apache.tajo.rpc.test.DummyProtocol", te.getProtocol());
      assertEquals(server.getListenAddress().getAddress().getHostAddress() + ":" + server.getListenAddress().getPort(),
          te.getRemoteAddress());
    }
  }

  @Test
  public void testConnectionRetry() throws Exception {
    retries = 10;
    final InetSocketAddress address = server.getListenAddress();
    tearDown();

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    //lazy startup
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(100);
          server = new BlockingRpcServer(DummyProtocol.class, service, address, 2);
        } catch (Exception e) {
          fail(e.getMessage());
        }
        server.start();
      }
    });
    serverThread.start();

    clientChannelFactory = RpcChannelFactory.createClientChannelFactory(MESSAGE, 2);
    client = new BlockingRpcClient(DummyProtocol.class, address, clientChannelFactory, retries);
    stub = client.getStub();

    EchoMessage response = stub.echo(null, message);
    assertEquals(MESSAGE, response.getMessage());
  }

  @Test
  public void testConnectionFailed() throws Exception {
    boolean expected = false;
    try {
      int port = server.getListenAddress().getPort() + 1;
      new BlockingRpcClient(DummyProtocol.class,
          RpcUtils.getConnectAddress(new InetSocketAddress("127.0.0.1", port)), clientChannelFactory, retries);
      fail("Connection should be failed.");
    } catch (ConnectException ce) {
      expected = true;
    } catch (Throwable ce){
      fail();
    }
    assertTrue(expected);
  }

  @Test
  public void testGetNull() throws Exception {
    assertNull(stub.getNull(null, null));
    assertTrue(service.getNullCalled);
  }

  @Test
  public void testShutdown() throws Exception {
    final StringBuilder error = new StringBuilder();
    Thread callThread = new Thread() {
      public void run() {
        try {
          EchoMessage message = EchoMessage.newBuilder()
              .setMessage(MESSAGE)
              .build();
          stub.deley(null, message);
        } catch (Exception e) {
          error.append(e.getMessage());
        }
        synchronized(error) {
          error.notifyAll();
        }
      }
    };

    callThread.start();

    final CountDownLatch latch = new CountDownLatch(1);
    Thread shutdownThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        try {
          server.shutdown();
          server = null;
          latch.countDown();
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
    shutdownThread.start();

    latch.await(5 * 1000, TimeUnit.MILLISECONDS);

    assertTrue(latch.getCount() == 0);

    synchronized(error) {
      error.wait(5 * 1000);
    }

    if(!error.toString().isEmpty()) {
      fail(error.toString());
    }
  }

  @Test
  public void testUnresolvedAddress() throws Exception {
    client.close();
    client = null;

    String hostAndPort = RpcUtils.normalizeInetSocketAddress(server.getListenAddress());
    client = new BlockingRpcClient(DummyProtocol.class,
        RpcUtils.createUnresolved(hostAndPort), clientChannelFactory, retries);
    BlockingInterface stub = client.getStub();

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    EchoMessage response2 = stub.echo(null, message);
    assertEquals(MESSAGE, response2.getMessage());
  }
}