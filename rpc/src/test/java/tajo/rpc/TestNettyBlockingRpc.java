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

public class TestNettyBlockingRpc {

  public static String MESSAGE = TestNettyBlockingRpc.class.getName();
  NettyRpcServer server;
  DummyClientInterface proxy;

  // !. Write Interface and implement class according to communication way
  public static interface DummyServerInterface {
    public MulResponse mul(MulRequest1 req1);

    public void nullParameterTest(NullProto proto);
  }

  public static interface DummyClientInterface {
    public void throwException(MulRequest1 request) throws RemoteException;

    public MulResponse mul(MulRequest1 req1) throws RemoteException;

    public void nullParameterTest(NullProto proto) throws RemoteException;
  }

  public static class DummyServer implements DummyServerInterface {
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
    // 2. Write Server Part source code
    server = NettyRpc.getProtoParamRpcServer(new DummyServer(),
        DummyServerInterface.class, new InetSocketAddress(0));
    server.start();

    InetSocketAddress addr = server.getBindAddress();
    Thread.sleep(100);

    // 3. Write client Part source code
    // 3.1 Make Proxy to make connection to server
    proxy = (DummyClientInterface) NettyRpc.getProtoParamBlockingRpcProxy(
        DummyClientInterface.class, addr);
  }

  @After
  public void tearDown() throws IOException {
    server.shutdown();
  }

  @Test
  public void testRpcProtoType() throws Exception {
    // 3.2 Fill request data
    MulRequest1 req1 = MulRequest1.newBuilder().setX1(10).setX2(20).build();

    // 3.3 call procedure
    MulResponse re = proxy.mul(req1);
    assertEquals(200, re.getResult1());
    assertEquals(400, re.getResult2());
  }

  @Test
  public void testNullParameter() throws Exception {
    NullProto np = NullProto.newBuilder().build();

    // 3.3 call procedure
    proxy.nullParameterTest(np);
  }
}
