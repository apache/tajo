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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
  RpcClientManager.RpcConnectionKey rpcConnectionKey;
  RpcClientManager manager = RpcClientManager.getInstance();

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
        service, new InetSocketAddress("127.0.0.1", 0), 3);
    server.start();
  }

  public void setUpRpcClient() throws Exception {
    retries = 1;

    rpcConnectionKey = new RpcClientManager.RpcConnectionKey(
        RpcUtils.getConnectAddress(server.getListenAddress()),
        DummyProtocol.class, true);
    client = manager.newClient(rpcConnectionKey, retries, 10, TimeUnit.SECONDS, true);
    assertTrue(client.isConnected());
    stub = client.getStub();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    RpcChannelFactory.shutdownGracefully();
  }

  public void tearDownRpcServer() throws Exception {
    if (server != null) {
      server.shutdown();
      server = null;
    }
  }

  public void tearDownRpcClient() throws Exception {
    if (client != null) {
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


    final CountDownLatch barrier = new CountDownLatch(1);
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    RpcCallback<EchoMessage> callback = new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        assertNotNull(parameter);
        echo = parameter.getMessage();
        assertEquals(MESSAGE, echo);
        calledMarker = true;
        barrier.countDown();
      }
    };
    stub.echo(null, echoMessage, callback);

    assertTrue(barrier.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(calledMarker);
  }

  @Test
  public void testGetNull() throws Exception {
    final CountDownLatch barrier = new CountDownLatch(1);
    stub.getNull(null, null, new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        assertNull(parameter);
        LOG.info("testGetNull retrieved");
        barrier.countDown();
      }
    });
    assertTrue(barrier.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(service.getNullCalled);
  }

  @Test
  public void testCallFuture() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.delay(future.getController(), echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(echoMessage, future.get());
    assertTrue(future.isDone());
  }

  @Test
  public void testCallFutureTimeout() throws Exception {
    boolean timeout = false;
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    EchoMessage echoMessage = EchoMessage.newBuilder().setMessage(MESSAGE).build();
    try {
      stub.delay(future.getController(), echoMessage, future);
      assertFalse(future.isDone());
      future.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      timeout = true;
    }
    assertFalse(future.getController().failed());
    assertTrue(timeout);
  }

  @Test
  public void testThrowException() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.throwException(future.getController(), echoMessage, future);

    assertFalse(future.isDone());
    EchoMessage result = null;
    try {
      result = future.get();
    } catch (ExecutionException e) {
    }

    assertEquals(null, result);
    assertTrue(future.isDone());
    assertTrue(future.getController().failed());
    assertNotNull(future.getController().errorText());
  }

  @Test
  public void testThrowException2() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.throwException(null, echoMessage, future);

    assertFalse(future.isDone());
    assertNull(future.get());

    assertTrue(future.isDone());
    assertFalse(future.getController().failed());
    assertNull(future.getController().errorText());
  }

  @Test(timeout = 60000)
  public void testServerShutdown1() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    tearDownRpcServer();

    stub.echo(future.getController(), echoMessage, future);

    EchoMessage result = null;
    try {
      result = future.get();
    } catch (ExecutionException e) {
    }

    assertEquals(null, result);
    assertTrue(future.isDone());
    assertTrue(future.getController().failed());
    assertNotNull(future.getController().errorText(), future.getController().errorText());
  }

  @Test(timeout = 60000)
  public void testServerShutdown2() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    tearDownRpcServer();

    Interface stub = client.getStub();
    stub.echo(future.getController(), echoMessage, future);

    EchoMessage result = null;
    try {
      result = future.get();
    } catch (ExecutionException e) {
    }

    assertEquals(null, result);
    assertTrue(future.isDone());
    assertTrue(future.getController().failed());
    assertNotNull(future.getController().errorText(), future.getController().errorText());
  }

  @Test
  @SetupRpcConnection(setupRpcServer = false, setupRpcClient = false)
  public void testClientRetryOnStartup() throws Exception {
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
    AsyncRpcClient client = manager.newClient(rpcConnectionKey,
        retries, 0, TimeUnit.MILLISECONDS, false);
    assertTrue(client.isConnected());

    Interface stub = client.getStub();
    stub.echo(future.getController(), echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(echoMessage, future.get());
    assertTrue(future.isDone());
    client.close();
    server.shutdown();
  }


  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcServer = false, setupRpcClient = false)
  public void testClientRetryFailureOnStartup() throws Exception {
    retries = 2;
    ServerSocket serverSocket = new ServerSocket(0);
    final InetSocketAddress address = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
    serverSocket.close();
    service = new DummyProtocolAsyncImpl();

    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, true);
    AsyncRpcClient client = new AsyncRpcClient(rpcConnectionKey, retries);
    try {
      client.connect();
      fail();
    } catch (ConnectTimeoutException e) {
      assertFalse(e.getMessage(), client.isConnected());
    }

    stub = client.getStub();
    stub.echo(future.getController(), echoMessage, future);

    EchoMessage result = null;
    try {
      result = future.get();
    } catch (ExecutionException e) {
    }

    assertEquals(null, result);
    assertTrue(future.isDone());
    assertTrue(future.getController().failed());
    assertNotNull(future.getController().errorText(), future.getController().errorText());
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcServer = false, setupRpcClient = false)
  public void testUnresolvedAddress() throws Exception {
    InetSocketAddress address = new InetSocketAddress("test", 0);
    boolean expected = false;
    AsyncRpcClient client = null;
    try {
      RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, true);
      client = new AsyncRpcClient(rpcConnectionKey, retries);
      client.connect();
      fail();
    } catch (ConnectException e) {
      expected = true;
    } catch (Throwable throwable) {
      fail(throwable.getMessage());
    } finally {
      client.close();
    }
    assertTrue(expected);

  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testUnresolvedAddress2() throws Exception {
    String hostAndPort = RpcUtils.normalizeInetSocketAddress(server.getListenAddress());
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(
            RpcUtils.createUnresolved(hostAndPort), DummyProtocol.class, true);
    AsyncRpcClient client = new AsyncRpcClient(rpcConnectionKey, retries);
    client.connect();
    try {
      assertTrue(client.isConnected());
      Interface stub = client.getStub();
      EchoMessage echoMessage = EchoMessage.newBuilder()
          .setMessage(MESSAGE).build();
      CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
      stub.echo(future.getController(), echoMessage, future);

      assertFalse(future.isDone());
      assertEquals(echoMessage, future.get());
      assertTrue(future.isDone());
    } finally {
      client.close();
    }
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testStubRecovery() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    AsyncRpcClient client = manager.newClient(rpcConnectionKey, 2, 0, TimeUnit.MILLISECONDS, false);

    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    int repeat = 5;

    assertTrue(client.isConnected());
    Interface stub = client.getStub();

    client.close(); // close connection
    assertFalse(client.isConnected());

    for (int i = 0; i < repeat; i++) {
      try {
        CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
        stub.echo(future.getController(), echoMessage, future);
        assertEquals(echoMessage, future.get());
        assertTrue(future.isDone());
        assertTrue(client.isConnected());
      } finally {
        client.close(); // close connection
        assertFalse(client.isConnected());
      }
    }
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testIdleTimeout() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    //500 millis idle timeout
    AsyncRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, false);
    assertTrue(client.isConnected());

    Thread.sleep(600);  //timeout
    assertFalse(client.isConnected());

    client.connect(); // try to reconnect
    assertTrue(client.isConnected());

    Thread.sleep(600);  //timeout
    assertFalse(client.isConnected());
    client.close();
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testPingOnIdle() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);

    //500 millis request timeout
    AsyncRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, true);
    assertTrue(client.isConnected());

    Thread.sleep(600);
    assertTrue(client.isConnected());

    Thread.sleep(600);
    assertTrue(client.isConnected());
    client.close();
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testIdleTimeoutWithActiveRequest() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);
    //500 millis idle timeout
    AsyncRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, false);
    assertTrue(client.isConnected());

    Interface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.delay(future.getController(), echoMessage, future); //3 sec delay
    assertTrue(client.isConnected());

    assertFalse(future.isDone());
    assertEquals(echoMessage, future.get());
    assertTrue(future.isDone());

    assertTrue(client.getActiveRequests() == 0);
    Thread.sleep(600);  //timeout
    assertFalse(client.isConnected());
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testRequestTimeoutOnBusy() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, true);

    //500 millis request timeout
    AsyncRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, true);
    assertTrue(client.isConnected());

    Interface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.busy(future.getController(), echoMessage, future); //30 sec delay
    assertFalse(future.isDone());

    EchoMessage result = null;
    try {
      result = future.get();
    } catch (ExecutionException e) {
    }

    assertEquals(null, result);
    assertTrue(future.getController().errorText(), future.getController().failed());
    assertTrue(client.getActiveRequests() == 0);
    client.close();
  }
}