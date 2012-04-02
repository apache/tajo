package nta.rpc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.rpc.test.DummyProtos.MulRequest1;
import nta.rpc.test.DummyProtos.MulResponse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
