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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.test.DummyProtocol;
import org.apache.tajo.rpc.test.DummyProtocol.DummyProtocolService.Interface;
import org.apache.tajo.rpc.test.TestProtos.EchoMessage;
import org.apache.tajo.rpc.test.TestProtos.SumRequest;
import org.apache.tajo.rpc.test.TestProtos.SumResponse;
import org.apache.tajo.rpc.test.impl.DummyProtocolAsyncImpl;
import org.apache.tajo.util.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class TestAsyncRpc {
  private static Log LOG = LogFactory.getLog(TestAsyncRpc.class);
  private static String MESSAGE = "TestAsyncRpc";

  double sum;
  String echo;

  static AsyncRpcServer server;
  static AsyncRpcClient client;
  static Interface stub;
  static DummyProtocolAsyncImpl service;

  @Before
  public void setUp() throws Exception {
    service = new DummyProtocolAsyncImpl();
    server = new AsyncRpcServer(DummyProtocol.class,
        service, new InetSocketAddress("127.0.0.1", 0), 2);
    server.start();
    client = new AsyncRpcClient(DummyProtocol.class,
        NetUtils.getConnectAddress(server.getListenAddress()));
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
    testNullLatch.await(1000, TimeUnit.MILLISECONDS);
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

  @Test(expected = TimeoutException.class)
  public void testCallFutureTimeout() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();
    stub.deley(null, echoMessage, future);

    assertFalse(future.isDone());
    assertEquals(future.get(1, TimeUnit.SECONDS), echoMessage);
  }

  @Test
  public void testCallFutureDisconnected() throws Exception {
    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    CallFuture<EchoMessage> future = new CallFuture<EchoMessage>();

    server.shutdown();
    server = null;

    stub.echo(future.getController(), echoMessage, future);
    EchoMessage response = future.get();

    assertNull(response);
    assertTrue(future.getController().failed());
    assertTrue(future.getController().errorText() != null);
  }
}