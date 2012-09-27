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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.test.DummyProtos.MulRequest1;
import tajo.rpc.test.DummyProtos.MulResponse;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

public class TestNettyAsyncRpc {
  public static String MESSAGE = TestNettyAsyncRpc.class.getName();
  NettyRpcServer server;
  DummyClientInterface proxy;

  public static interface DummyServerInterface {
    public Object throwException(MulRequest1 request) throws IOException;

    public MulResponse mul(MulRequest1 req1);

    public void nullParameterTest(NullProto proto);
  }

  public static interface DummyClientInterface {
    public void throwException(Callback<Object> callback, MulRequest1 request)
        throws IOException;

    public void mul(Callback<MulResponse> callback, MulRequest1 req1);

    public void nullParameterTest(Callback<Object> callback, NullProto proto);
  }

  public static class DummyServer implements DummyServerInterface {
    @Override
    public Object throwException(MulRequest1 request) throws IOException {
      throw new IOException();
    }

    @Override
    public MulResponse mul(MulRequest1 req1) {
      int x1_1 = req1.getX1();
      int x1_2 = req1.getX2();

      int result1 = x1_1 * x1_2;

      MulResponse rst = MulResponse.newBuilder().setResult1(result1)
          .setResult2(400).build();
      return rst;
    }

    public void nullParameterTest(NullProto proto) {
    }
  }

  @Before
  public void setUp() throws Exception {
    server = NettyRpc.getProtoParamRpcServer(new DummyServer(),
        DummyServerInterface.class, new InetSocketAddress(0));
    server.start();

    InetSocketAddress addr = server.getBindAddress();

    proxy = (DummyClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
        DummyServerInterface.class, DummyClientInterface.class, addr);
  }

  @After
  public void tearDown() throws IOException {
    server.shutdown();
  }

  MulResponse answer1 = null;

  @Test
  public void testRpcProtoType() throws Exception {
    MulRequest1 req1 = MulRequest1.newBuilder().setX1(10).setX2(20).build();

    Callback<MulResponse> cb = new Callback<MulResponse>();
    proxy.mul(cb, req1);

    MulResponse resp = (MulResponse) cb.get();
    assertEquals(200, resp.getResult1());
    assertEquals(400, resp.getResult2());
  }

  @Test
  public void testNullParameter() throws Exception {
    NullProto np = NullProto.newBuilder().build();

    Callback<Object> cb = new Callback<Object>();
    proxy.nullParameterTest(cb, np);
  }
}
