package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import nta.rpc.test.DummyProtos.MulRequest;
import nta.rpc.test.DummyProtos.MulResponse;

public class TestProtoParamAsyncRpc {
  public static String MESSAGE = TestProtoParamAsyncRpc.class.getName();
  ProtoParamRpcServer server;
  DummyClientInterface proxy;

  public static interface DummyServerInterface {
    public MulResponse mul(MulRequest request);

    public Object throwException(MulRequest request) throws IOException;

    public Integer primitiveTypeTest(String inStr);
  }

  public static interface DummyClientInterface {
    public void mul(Callback<MulResponse> callback, MulRequest request);

    public void throwException(Callback<Object> callback, MulRequest request)
        throws IOException;

    public void primitiveTypeTest(Callback<Integer> callback, String inStr);
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
    public Object throwException(MulRequest request) throws IOException {
      throw new IOException();
    }

    @Override
    public Integer primitiveTypeTest(String inStr) {
      System.out.println("Server" + inStr);
      return 10;
    }
  }

  @Before
  public void setUp() throws Exception {
    server =
        NettyRpc.getProtoParamRpcServer(new DummyServer(),
            new InetSocketAddress(0));
    server.start();

    InetSocketAddress addr = server.getBindAddress();

    proxy =
        (DummyClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
            DummyServerInterface.class, DummyClientInterface.class, addr);
  }

  @After
  public void tearDown() throws IOException {
    server.shutdown();
  }

  MulResponse answer1 = null;

  @Test
  public void testRpcProtoType() throws Exception {

    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    System.out.println("Do whatever you want before get result!!");

    try {
      Callback<MulResponse> cb = new Callback<MulResponse>();
      proxy.mul(cb, req);

      MulResponse resp = (MulResponse) cb.get();
      assertEquals(200, resp.getResult());

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

  @Test
  public void testRpcPrimitiveType() throws Exception {
    System.out.println("Do whatever you want before get result!!");

    try {
      Callback<Integer> cb = new Callback<Integer>();
      proxy.primitiveTypeTest(cb, "TEST STRING");

      Integer x = cb.get();
      System.out.println(x);

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

  @Test
  public void testRpcRemoteException() throws Exception {

    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    System.out.println("Do whatever you want before get result!!");

    try {
      Callback<Object> cb = new Callback<Object>();
      proxy.throwException(cb, req);
      cb.get();

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

}
