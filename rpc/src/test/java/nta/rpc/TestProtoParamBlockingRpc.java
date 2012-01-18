package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.rpc.test.DummyProtos.MulRequest;
import nta.rpc.test.DummyProtos.MulResponse;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestProtoParamBlockingRpc {

  public static String MESSAGE = TestProtoParamBlockingRpc.class.getName();

  // !. Write Interface and implement class according to communication way
  public static interface DummyServerInterface {
    public MulResponse mul(MulRequest request);

    public void throwException(MulRequest request) throws IOException;
  }

  public static interface DummyClientInterface {
    public MulResponse mul(MulRequest request) throws RemoteException;

    public void throwException(MulRequest request) throws RemoteException;
  }

  public static class DummyServer implements DummyServerInterface {
    @Override
    public MulResponse mul(MulRequest request) {
      int x1 = request.getX1();
      int x2 = request.getX2();

      int result = x1 * x2;

      MulResponse rst = MulResponse.newBuilder().setResult(result).build();
      return rst;
    }

    @Override
    public void throwException(MulRequest request) throws IOException {
      throw new IOException();
    }
  }

  // @Test
  public void testRpc() throws Exception {

    // 2. Write Server Part source code
    ProtoParamRpcServer server =
        NettyRpc.getProtoParamRpcServer(new DummyServer(),
            new InetSocketAddress(10010));
    server.start();
    Thread.sleep(100);

    // 3. Write client Part source code
    // 3.1 Make Proxy to make connection to server
    DummyServerInterface proxy =
        (DummyServerInterface) NettyRpc.getProtoParamBlockingRpcProxy(
            DummyServerInterface.class, new InetSocketAddress(10010));

    // 3.2 Fill request data
    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    // 3.3 call procedure
    MulResponse re = proxy.mul(req);
    assertEquals(200, re.getResult());

    server.shutdown();
  }

  @Test
  public void testRpcRandomPort() throws Exception {

    // 2. Write Server Part source code
    ProtoParamRpcServer server =
        NettyRpc.getProtoParamRpcServer(new DummyServer(),
            new InetSocketAddress(0));
    server.start();

    InetSocketAddress addr = server.getBindAddress();
    Thread.sleep(100);

    // 3. Write client Part source code
    // 3.1 Make Proxy to make connection to server
    DummyClientInterface proxy =
        (DummyClientInterface) NettyRpc.getProtoParamBlockingRpcProxy(
            DummyClientInterface.class, addr);

    // 3.2 Fill request data
    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    // 3.3 call procedure
    try {
      MulResponse re = proxy.mul(req);
      assertEquals(200, re.getResult());

      proxy.throwException(req);
    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

    server.shutdown();
  }

}
