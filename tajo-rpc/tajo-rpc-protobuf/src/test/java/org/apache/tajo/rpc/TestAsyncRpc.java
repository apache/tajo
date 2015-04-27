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

import com.google.protobuf.RpcCallback;
import io.netty.channel.ConnectTimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.test.DummyProtocol;
import org.apache.tajo.rpc.test.DummyProtocol.DummyProtocolService.Interface;
import org.apache.tajo.rpc.test.TestProtos.EchoMessage;
import org.apache.tajo.rpc.test.TestProtos.SumRequest;
import org.apache.tajo.rpc.test.TestProtos.SumResponse;
import org.apache.tajo.rpc.test.impl.DummyProtocolAsyncImpl;
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
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class TestAsyncRpc {
  private static Log LOG = LogFactory.getLog(TestAsyncRpc.class);
  private static String MESSAGE = "TestAsyncRpc";

  double sum;
  String echo;

  AsyncRpcServer server;
  AsyncRpcClient client;
  Interface stub;
  DummyProtocolAsyncImpl service;
  int retries;
  
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
    service = new DummyProtocolAsyncImpl();
    server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), 2);
    server.start();
  }
  
  public void setUpRpcClient() throws Exception {
    retries = 1;

    RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(
              RpcUtils.getConnectAddress(server.getListenAddress()),
              DummyProtocol.class, true);
    client = new AsyncRpcClient(rpcConnectionKey, retries);
    client.connect();
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

  boolean calledMarker = false;

  @Test
  public void testRpc() throws Exception {

    SumRequest sumRequest = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();

    stub.sum(null, sumRequest, new RpcCallback<SumResponse>() {
      @Override
      public void run(SumResponse parameter) {
        sum = parameter.getResult();
        assertTrue(8.15d == sum);
      }
    });


    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    RpcCallback<EchoMessage> callback = new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        echo = parameter.getMessage();
        assertEquals(MESSAGE, echo);
        calledMarker = true;
      }
    };
    stub.echo(null, echoMessage, callback);
    Thread.sleep(1000);
    assertTrue(calledMarker);
  }

  private CountDownLatch testNullLatch;

  @Test
  public void testGetNull() throws Exception {
    testNullLatch = new CountDownLatch(1);
    stub.getNull(null, null, new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        assertNull(parameter);
        LOG.info("testGetNull retrieved");
        testNullLatch.countDown();
      }
    });
    assertTrue(testNullLatch.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(service.getNullCalled);
  }

  @Test
  public void testCallFuture() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.deley(null, echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(future.get(), echoMessage);
    assertTrue(future.isDone());
  }

  @Test
  public void testCallFutureTimeout() throws Exception {
    boolean timeout = false;
    try {
      EchoMessage echoMessage = EchoMessage.newBuilder()
          .setMessage(MESSAGE).build();
      CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
      stub.deley(null, echoMessage, future);

      assertFalse(future.isDone());
      future.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      timeout = true;
    }
    assertTrue(timeout);
  }

  @Test
  public void testCallFutureDisconnected() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    tearDownRpcServer();

    stub.echo(future.getController(), echoMessage, future);
    EchoMessage response = future.get();

    assertNull(response);
    assertTrue(future.getController().failed());
    assertTrue(future.getController().errorText() != null);
  }

  @Test
  public void testStubDisconnected() throws Exception {

    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    if (server != null) {
      server.shutdown(true);
      server = null;
    }

    stub = client.getStub();
    stub.echo(future.getController(), echoMessage, future);
    EchoMessage response = future.get();

    assertNull(response);
    assertTrue(future.getController().failed());
    assertTrue(future.getController().errorText() != null);
  }

  @Test
  @SetupRpcConnection(setupRpcServer=false,setupRpcClient=false)
  public void testConnectionRetry() throws Exception {
    retries = 10;
    ServerSocket serverSocket = new ServerSocket(0);
    final InetSocketAddress address = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
    serverSocket.close();
    service = new DummyProtocolAsyncImpl();

    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    //lazy startup
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          server = new AsyncRpcServer(DummyProtocol.class,
              service, address, 2);
        } catch (Exception e) {
          fail(e.getMessage());
        }
        server.start();
      }
    });
    serverThread.start();

    RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, true);
    client = new AsyncRpcClient(rpcConnectionKey, retries);
    client.connect();
    assertTrue(client.isConnected());
    stub = client.getStub();
    stub.echo(future.getController(), echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(echoMessage, future.get());
    assertTrue(future.isDone());
  }

  @Test
  public void testConnectionFailure() throws Exception {
    InetSocketAddress address = new InetSocketAddress("test", 0);
    boolean expected = false;
    try {
      RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, true);
      NettyClientBase client = new AsyncRpcClient(rpcConnectionKey, retries);
      client.connect();
      fail();
    } catch (ConnectTimeoutException e) {
      expected = true;
    } catch (Throwable throwable) {
      fail();
    }
    assertTrue(expected);

  }

  @Test
  @SetupRpcConnection(setupRpcClient=false)
  public void testUnresolvedAddress() throws Exception {
    String hostAndPort = RpcUtils.normalizeInetSocketAddress(server.getListenAddress());
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(
              RpcUtils.createUnresolved(hostAndPort), DummyProtocol.class, true);
    client = new AsyncRpcClient(rpcConnectionKey, retries);
    client.connect();
    assertTrue(client.isConnected());
    Interface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.deley(null, echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(future.get(), echoMessage);
    assertTrue(future.isDone());
  }

  @Test
  public void testIdleTimeout() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    AsyncRpcClient client = new AsyncRpcClient(rpcConnectionKey, retries, 1); //1 sec idle timeout
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
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    AsyncRpcClient client = new AsyncRpcClient(rpcConnectionKey, retries, 1); //1 sec idle timeout
    client.connect();

    assertTrue(client.isConnected());
    Interface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.deley(null, echoMessage, future); //3 sec delay

    assertFalse(future.isDone());
    assertEquals(future.get(), echoMessage);
    assertTrue(future.isDone());

    Thread.sleep(2000);
    assertFalse(client.isConnected());
  }
}