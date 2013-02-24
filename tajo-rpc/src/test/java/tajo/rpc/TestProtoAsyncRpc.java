/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.rpc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.rpc.test.DummyProtocol;
import tajo.rpc.test.DummyProtocol.DummyProtocolService.Interface;
import tajo.rpc.test.TestProtos.EchoMessage;
import tajo.rpc.test.TestProtos.SumRequest;
import tajo.rpc.test.TestProtos.SumResponse;
import tajo.rpc.test.impl.DummyProtocolAsyncImpl;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class TestProtoAsyncRpc {
  private static Log LOG = LogFactory.getLog(TestProtoAsyncRpc.class);
  private static String MESSAGE = "TestProtoAsyncRpc";

  double sum;
  String echo;

  static ProtoAsyncRpcServer server;
  static ProtoAsyncRpcClient client;
  static Interface stub;
  static DummyProtocolAsyncImpl service;

  @Before
  public void setUp() throws Exception {
    service = new DummyProtocolAsyncImpl();
    server = new ProtoAsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress(0));
    server.start();
    client = new ProtoAsyncRpcClient(DummyProtocol.class, server.getBindAddress());
    stub = client.getStub();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    server.shutdown();
  }

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

    stub.echo(null, echoMessage, new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        echo = parameter.getMessage();
        assertEquals(MESSAGE, echo);
      }
    });
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
    testNullLatch.await(1000, TimeUnit.MILLISECONDS);
    assertTrue(service.getNullCalled);
  }

  private CountDownLatch testGetErrorLatch;

  //@Test
  // TODO - to be fixed
  public void testGetError() throws Exception {
    EchoMessage echoMessage2 = EchoMessage.newBuilder()
        .setMessage("[Don't Worry! It's an exception message for unit test]").
            build();

    testGetErrorLatch = new CountDownLatch(1);
    RpcController controller = new NettyRpcController();
    stub.getError(controller, echoMessage2, new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        assertNull(parameter);
        LOG.info("testGetError retrieved");
        testGetErrorLatch.countDown();
      }
    });
    testGetErrorLatch.await(1000, TimeUnit.MILLISECONDS);
    assertTrue(service.getErrorCalled);
    assertTrue(controller.failed());
    assertEquals(echoMessage2.getMessage(), controller.errorText());
  }

  @Test
  public void testCallFuture() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture2<EchoMessage> future = new CallFuture2<EchoMessage>();
    stub.deley(null, echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(future.get(), echoMessage);
    assertTrue(future.isDone());
  }

  @Test(expected = TimeoutException.class)
  public void testCallFutureTimeout() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture2<EchoMessage> future = new CallFuture2<EchoMessage>();
    stub.deley(null, echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(future.get(1, TimeUnit.SECONDS), echoMessage);
  }
}