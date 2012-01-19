package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.rpc.TestProtoParamAsyncRpc.DummyClientInterface;
import nta.rpc.test.DummyProtos.MulRequest;
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
    public MulResponse mul(MulRequest request);

    public void throwException(MulRequest request) throws IOException;

    public int primitiveTypeTest(String inStr, int value);
  }

  public static interface DummyClientInterface {
    public MulResponse mul(MulRequest request) throws RemoteException;

    public void throwException(MulRequest request) throws RemoteException;

    public int primitiveTypeTest(String inStr, int value);
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

    @Override
    public int primitiveTypeTest(String inStr, int value) {
      System.out.println("Server" + inStr);
      return value;
    }
  }

  @Before
  public void setUp() throws Exception {
    // 2. Write Server Part source code
    server =
        NettyRpc.getProtoParamRpcServer(new DummyServer(),
            new InetSocketAddress(0));
    server.start();

    InetSocketAddress addr = server.getBindAddress();
    Thread.sleep(100);

    // 3. Write client Part source code
    // 3.1 Make Proxy to make connection to server
    proxy =
        (DummyClientInterface) NettyRpc.getProtoParamBlockingRpcProxy(
            DummyClientInterface.class, addr);
  }

  @After
  public void tearDown() throws IOException {
    server.shutdown();
  }

  @Test
  public void testRpcProtoType() throws Exception {

    // 3.2 Fill request data
    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    // 3.3 call procedure
    try {
      MulResponse re = proxy.mul(req);
      assertEquals(200, re.getResult());
      
    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testRpcPrimitiveType() throws Exception {

    try {
      int retValue = proxy.primitiveTypeTest("TEST STRING", 10);
      assertEquals(10, retValue);
      
    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

  @Test
  public void testRpcRemoteException() throws Exception {
    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    try {
      proxy.throwException(req);
    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

    server.shutdown();
  }

}
