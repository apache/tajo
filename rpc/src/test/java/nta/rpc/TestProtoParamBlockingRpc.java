package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.rpc.test.DummyProtos.MulRequest1;
import nta.rpc.test.DummyProtos.MulResponse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestProtoParamBlockingRpc {

  public static String MESSAGE = TestProtoParamBlockingRpc.class.getName();
  ProtoParamRpcServer server;
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
