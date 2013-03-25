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

package tajo.rpc;

import com.google.protobuf.RpcController;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.rpc.test.DummyProtocol;
import tajo.rpc.test.DummyProtocol.DummyProtocolService.BlockingInterface;
import tajo.rpc.test.TestProtos.EchoMessage;
import tajo.rpc.test.TestProtos.SumRequest;
import tajo.rpc.test.TestProtos.SumResponse;
import tajo.rpc.test.impl.DummyProtocolBlockingImpl;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;

public class TestProtoBlockingRpc {
  public static String MESSAGE = "TestProtoBlockingRpc";

  private static ProtoBlockingRpcServer server;
  private static ProtoBlockingRpcClient client;
  private static BlockingInterface stub;
  private static DummyProtocolBlockingImpl service;

  @BeforeClass
  public static void setUp() throws Exception {
    service = new DummyProtocolBlockingImpl();
    server = new ProtoBlockingRpcServer(DummyProtocol.class, service,
        new InetSocketAddress(10000));
    server.start();

    client = new ProtoBlockingRpcClient(DummyProtocol.class,
        new InetSocketAddress(10000));
    stub = client.getStub();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
    server.shutdown();
  }

  @Test
  public void testRpc() throws Exception {
    SumRequest request = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();
    SumResponse response1 = stub.sum(null, request);
    assertTrue(8.15d == response1.getResult());

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

  //@Test
  public void testGetError() throws Exception {
    EchoMessage echoMessage2 = EchoMessage.newBuilder()
        .setMessage("[Don't Worry! It's an exception message for unit test]").
            build();

    RpcController controller = new NettyRpcController();
    assertNull(stub.getError(controller, echoMessage2));
    assertTrue(service.getErrorCalled);
    assertTrue(controller.failed());
    assertEquals(echoMessage2.getMessage(), controller.errorText());
  }
}