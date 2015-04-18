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

import io.netty.channel.ConnectTimeoutException;
import org.apache.tajo.rpc.test.DummyProtocol;
import org.apache.tajo.rpc.test.DummyProtocol.DummyProtocolService.BlockingInterface;
import org.apache.tajo.rpc.test.TestProtos.EchoMessage;
import org.apache.tajo.rpc.test.TestProtos.SumRequest;
import org.apache.tajo.rpc.test.TestProtos.SumResponse;
import org.apache.tajo.rpc.test.impl.DummyProtocolBlockingImpl;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
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
  
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @interface SetupRpcConnection {
    boolean setupRpcServer() default true;
    boolean setupRpcClient() default true;
  }
  
  @Rule
  public ExternalResource resource = new ExternalResource() {
    
    private Description description;

    @Override
    public Statement apply(Statement base, Description description) {
      this.description = description;
      return super.apply(base, description);
    }

    @Override
    protected void before() throws Throwable {
      SetupRpcConnection setupRpcConnection = description.getAnnotation(SetupRpcConnection.class);
      
      if (setupRpcConnection == null || setupRpcConnection.setupRpcServer()) {
        setUpRpcServer();
      }
      
      if (setupRpcConnection == null || setupRpcConnection.setupRpcClient()) {
        setUpRpcClient();
      }
    }

    @Override
    protected void after() {
      SetupRpcConnection setupRpcConnection = description.getAnnotation(SetupRpcConnection.class);
      
      if (setupRpcConnection == null || setupRpcConnection.setupRpcClient()) {
        try {
          tearDownRpcClient();
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
      
      if (setupRpcConnection == null || setupRpcConnection.setupRpcServer()) {
        try {
          tearDownRpcServer();
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    }
    
  };
  
  public void setUpRpcServer() throws Exception {
    service = new DummyProtocolBlockingImpl();
    server = new BlockingRpcServer(DummyProtocol.class, service,
        new InetSocketAddress("127.0.0.1", 0), 2);
    server.start();
  }
  
  public void setUpRpcClient() throws Exception {
    retries = 1;

    RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(
              RpcUtils.getConnectAddress(server.getListenAddress()),
              DummyProtocol.class, false);
    client = new BlockingRpcClient(rpcConnectionKey, retries);
    client.connect();
    assertTrue(client.isConnected());
    stub = client.getStub();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    RpcChannelFactory.shutdownGracefully();
  }
  
  public void tearDownRpcServer() throws Exception {
    if(server != null) {
      server.shutdown();
      server = null;
    }
  }
  
  public void tearDownRpcClient() throws Exception {
    if(client != null) {
      client.close();
      client = null;
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
  @SetupRpcConnection(setupRpcClient=false)
  public void testRpcWithServiceCallable() throws Exception {
    RpcClientManager manager = RpcClientManager.getInstance();
    final SumRequest request = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();

    SumResponse response =
    new ServerCallable<SumResponse>(manager,
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
        new ServerCallable<SumResponse>(manager,
            server.getListenAddress(), DummyProtocol.class, false) {
          @Override
          public SumResponse call(NettyClientBase client) throws Exception {
            BlockingInterface stub2 = client.getStub();
            SumResponse response1 = stub2.sum(null, request);
            return response1;
          }
        }.withoutRetries();

    assertTrue(8.15d == response.getResult());
    RpcClientManager.close();
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
  public void testThrowException2() throws Exception {
    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    try {
      stub.throwException(null, message);
      fail("RpcCall should throw exception");
    } catch (Throwable t) {
      assertTrue(t instanceof TajoServiceException);
    }

    EchoMessage message1 = stub.deley(null, message);
    assertEquals(message, message1);
  }

  @Test
  @SetupRpcConnection(setupRpcServer=false,setupRpcClient=false)
  public void testConnectionRetry() throws Exception {
    retries = 10;
    ServerSocket serverSocket = new ServerSocket(0);
    final InetSocketAddress address = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
    serverSocket.close();

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    //lazy startup
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          server = new BlockingRpcServer(DummyProtocol.class, new DummyProtocolBlockingImpl(), address, 2);
        } catch (Exception e) {
          fail(e.getMessage());
        }
        server.start();
      }
    });
    serverThread.start();

    RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, false);
    client = new BlockingRpcClient(rpcConnectionKey, retries);
    client.connect();
    assertTrue(client.isConnected());
    stub = client.getStub();

    EchoMessage response = stub.echo(null, message);
    assertEquals(MESSAGE, response.getMessage());
  }

  @Test
  public void testConnectionFailed() throws Exception {
    NettyClientBase client = null;
    boolean expected = false;
    try {
      int port = server.getListenAddress().getPort() + 1;
      RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(
              RpcUtils.getConnectAddress(new InetSocketAddress("127.0.0.1", port)),
              DummyProtocol.class, false);
      client = new BlockingRpcClient(rpcConnectionKey, retries);
      client.connect();
      fail();
    } catch (ConnectTimeoutException e) {
      expected = true;
    } catch (Throwable e) {
      fail(e.getMessage());
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

    assertTrue(latch.await(5 * 1000, TimeUnit.MILLISECONDS));

    assertTrue(latch.getCount() == 0);

    synchronized(error) {
      error.wait(5 * 1000);
    }

    if(!error.toString().isEmpty()) {
      fail(error.toString());
    }
  }

  @Test
  @SetupRpcConnection(setupRpcClient=false)
  public void testUnresolvedAddress() throws Exception {
    String hostAndPort = RpcUtils.normalizeInetSocketAddress(server.getListenAddress());
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(
              RpcUtils.createUnresolved(hostAndPort), DummyProtocol.class, false);
    client = new BlockingRpcClient(rpcConnectionKey, retries);
    client.connect();
    assertTrue(client.isConnected());
    BlockingInterface stub = client.getStub();

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    EchoMessage response2 = stub.echo(null, message);
    assertEquals(MESSAGE, response2.getMessage());
  }

  @Test
  public void testIdleTimeout() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);
    BlockingRpcClient client = new BlockingRpcClient(rpcConnectionKey, retries, 1); //1 sec idle timeout
    client.connect();
    assertTrue(client.isConnected());

    Thread.sleep(2000);
    assertFalse(client.isConnected());

    client.connect(); // try to reconnect
    assertTrue(client.isConnected());
    client.close();
    assertFalse(client.isConnected());
  }

  @Test
  public void testIdleTimeoutWithActiveRequest() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);
    BlockingRpcClient client = new BlockingRpcClient(rpcConnectionKey, retries, 1); //1 sec idle timeout

    client.connect();

    assertTrue(client.isConnected());
    BlockingInterface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    EchoMessage message = stub.deley(null, echoMessage); //3 sec delay
    assertEquals(message, echoMessage);

    Thread.sleep(2000);
    assertFalse(client.isConnected());
  }
}