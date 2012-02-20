package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.rpc.test.DummyProtos.MulRequest1;
import nta.rpc.test.DummyProtos.MulResponse;

public class TestProtoParamAsyncRpc {
  public static String MESSAGE = TestProtoParamAsyncRpc.class.getName();
  ProtoParamRpcServer server;
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

      MulResponse rst =
          MulResponse.newBuilder().setResult1(result1).setResult2(400).build();
      return rst;
    }

    public void nullParameterTest(NullProto proto) {
      System.out.println("NULL PARAMETER TEST");
    }
  }

  @Before
  public void setUp() throws Exception {
    server =
        NettyRpc.getProtoParamRpcServer(new DummyServer(),
            DummyServerInterface.class, new InetSocketAddress(0));
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
  public void testRpcRemoteException() throws Exception {

    MulRequest1 req = MulRequest1.newBuilder().setX1(10).setX2(20).build();

    System.out.println("Do whatever you want before get result!!");

    try {
      Callback<Object> cb = new Callback<Object>();
      proxy.throwException(cb, req);
      cb.get();

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

  @Test
  public void testRpcProtoType() throws Exception {

    MulRequest1 req1 = MulRequest1.newBuilder().setX1(10).setX2(20).build();

    System.out.println("Do whatever you want before get result!!");

    try {
      Callback<MulResponse> cb = new Callback<MulResponse>();
      proxy.mul(cb, req1);

      MulResponse resp = (MulResponse) cb.get();
      assertEquals(200, resp.getResult1());
      assertEquals(400, resp.getResult2());

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

  @Test
  public void testNullParameter() throws Exception {
    NullProto np = NullProto.newBuilder().build();
    System.out.println("Do whatever you want before get result!!");

    try {
      Callback<Object> cb = new Callback<Object>();
      proxy.nullParameterTest(cb, np);

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

  }

}
