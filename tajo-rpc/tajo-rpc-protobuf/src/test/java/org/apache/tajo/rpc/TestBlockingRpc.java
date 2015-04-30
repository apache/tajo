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
import java.net.ConnectException;
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
  RpcClientManager manager = RpcClientManager.getInstance();
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
  public void testGetNull() throws Exception {
    assertNull(stub.getNull(null, null));
    assertTrue(service.getNullCalled);
  }

  @Test
  public void testThrowException() throws Exception {
    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    try {
      stub.throwException(null, message);
      fail("RpcCall should throw exception");
    } catch (TajoServiceException te) {
      assertEquals("Exception Test", te.getMessage());
      assertEquals("org.apache.tajo.rpc.test.DummyProtocol", te.getProtocol());
      assertEquals(server.getListenAddress().getAddress().getHostAddress() + ":" + server.getListenAddress().getPort(),
          te.getRemoteAddress());
    }
  }

  @Test
  public void testThrowException2() throws Exception {
    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    DefaultRpcController controller = new DefaultRpcController();
    try {
      stub.throwException(controller, message);
      fail("RpcCall should throw exception");
    } catch (TajoServiceException t) {
      assertTrue(controller.failed());
      assertNotNull(controller.errorText());
    }

    controller.reset();
    EchoMessage message1 = stub.delay(controller, message);
    assertEquals(message, message1);
    assertFalse(controller.failed());
    assertNull(controller.errorText());
  }

  @Test(timeout = 60000)
  public void testServerShutdown1() throws Exception {
    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    tearDownRpcServer();
    boolean expect = false;
    try {
      EchoMessage response = stub.echo(null, message);
      fail(response.getMessage());
    } catch (TajoServiceException e) {
      expect = true;
    }
    assertTrue(expect);
  }

  @Test(timeout = 60000)
  public void testServerShutdown2() throws Exception {
    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    tearDownRpcServer();
    boolean expect = false;
    try {
      BlockingInterface stub = client.getStub();
      EchoMessage response = stub.echo(null, message);
      fail(response.getMessage());
    } catch (TajoServiceException e) {
      expect = true;
    }
    assertTrue(expect);
  }

  @Test
  public void testServerShutdown3() throws Exception {
    final StringBuilder error = new StringBuilder();
    Thread callThread = new Thread() {
      public void run() {
        try {
          EchoMessage message = EchoMessage.newBuilder()
              .setMessage(MESSAGE)
              .build();
          stub.delay(null, message);
        } catch (Exception e) {
          error.append(e.getMessage());
        }
        synchronized (error) {
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

    synchronized (error) {
      error.wait(5 * 1000);
    }

    if (!error.toString().isEmpty()) {
      fail(error.toString());
    }
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcServer = false, setupRpcClient = false)
  public void testClientRetryOnStartup() throws Exception {
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

    BlockingRpcClient client = manager.newClient(rpcConnectionKey,
        retries, 0, TimeUnit.MILLISECONDS, false);
    assertTrue(client.isConnected());

    BlockingInterface stub = client.getStub();
    EchoMessage response = stub.echo(null, message);
    assertEquals(MESSAGE, response.getMessage());
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

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, false);
    BlockingRpcClient client = new BlockingRpcClient(rpcConnectionKey, retries);

    try {
      client.connect();
      fail();
    } catch (ConnectTimeoutException e) {
      assertFalse(e.getMessage(), client.isConnected());
    }

    BlockingInterface stub = client.getStub();
    try {
      EchoMessage response = stub.echo(null, message);
      fail();
    } catch (TajoServiceException e) {
      assertFalse(e.getMessage(), client.isConnected());
    }
    RpcClientManager.cleanup(client);
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcServer = false, setupRpcClient = false)
  public void testUnresolvedAddress() throws Exception {
    InetSocketAddress address = new InetSocketAddress("test", 0);
    boolean expected = false;
    BlockingRpcClient client = null;
    try {
      RpcClientManager.RpcConnectionKey rpcConnectionKey =
          new RpcClientManager.RpcConnectionKey(address, DummyProtocol.class, true);
      client = new BlockingRpcClient(rpcConnectionKey, retries);
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

  @Test
  @SetupRpcConnection(setupRpcClient = false)
  public void testUnresolvedAddress2() throws Exception {
    String hostAndPort = RpcUtils.normalizeInetSocketAddress(server.getListenAddress());
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(
            RpcUtils.createUnresolved(hostAndPort), DummyProtocol.class, false);
    BlockingRpcClient client = new BlockingRpcClient(rpcConnectionKey, retries);
    client.connect();
    assertTrue(client.isConnected());

    try {
      BlockingInterface stub = client.getStub();
      EchoMessage message = EchoMessage.newBuilder()
          .setMessage(MESSAGE).build();
      EchoMessage response2 = stub.echo(null, message);
      assertEquals(MESSAGE, response2.getMessage());
    } finally {
      client.close();
    }
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testStubRecovery() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);
    BlockingRpcClient client = manager.newClient(rpcConnectionKey, 1, 0, TimeUnit.MILLISECONDS, false);

    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    int repeat = 5;

    assertTrue(client.isConnected());
    BlockingInterface stub = client.getStub();

    client.close(); // close connection
    assertFalse(client.isConnected());

    for (int i = 0; i < repeat; i++) {
      try {
        EchoMessage response = stub.echo(null, echoMessage);
        assertEquals(MESSAGE, response.getMessage());
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
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);
    //500 millis idle timeout
    BlockingRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, false);
    assertTrue(client.isConnected());

    Thread.sleep(600);   //timeout
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
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);

    //500 millis request timeout
    BlockingRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, true);
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
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);
    //500 millis idle timeout
    BlockingRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, false);
    assertTrue(client.isConnected());

    BlockingInterface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    EchoMessage message = stub.delay(null, echoMessage); //3 sec delay
    assertEquals(message, echoMessage);
    assertTrue(client.isConnected());

    assertTrue(client.getActiveRequests() == 0);
    Thread.sleep(600);  //timeout
    assertFalse(client.isConnected());
  }

  @Test(timeout = 60000)
  @SetupRpcConnection(setupRpcClient = false)
  public void testRequestTimeoutOnBusy() throws Exception {
    RpcClientManager.RpcConnectionKey rpcConnectionKey =
        new RpcClientManager.RpcConnectionKey(server.getListenAddress(), DummyProtocol.class, false);

    //500 millis request timeout
    BlockingRpcClient client = manager.newClient(rpcConnectionKey, retries, 500, TimeUnit.MILLISECONDS, true);
    assertTrue(client.isConnected());

    BlockingInterface stub = client.getStub();
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    boolean expected = false;
    try {
      EchoMessage message = stub.busy(null, echoMessage); //30 sec delay
      fail();
    } catch (TajoServiceException e) {
      expected = true;
    } finally {
      client.close();
    }
    assertTrue(expected);
  }
}