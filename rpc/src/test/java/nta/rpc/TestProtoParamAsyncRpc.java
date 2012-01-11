package nta.rpc;

import java.net.InetSocketAddress;

import org.junit.Test;
import static org.junit.Assert.*;

import nta.rpc.test.DummyProtos.MulRequest;
import nta.rpc.test.DummyProtos.MulResponse;

public class TestProtoParamAsyncRpc {
  public static String MESSAGE = TestProtoParamAsyncRpc.class.getName();

  public static interface DummyServerInterface {
    public MulResponse mul(MulRequest request);
  }

  public static interface DummyClientInterface {
    public void mul(Callback<MulResponse> callback, MulRequest request);
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
  }

  MulResponse answer1 = null;

  @Test
  public void testRpc() throws Exception {
    ProtoParamRpcServer server =
        NettyRpc.getProtoParamRpcServer(new DummyServer(),
            new InetSocketAddress(10011));
    server.start();

    DummyClientInterface proxy =
        (DummyClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
            DummyServerInterface.class, DummyClientInterface.class,
            new InetSocketAddress(10011));

    MulRequest req = MulRequest.newBuilder().setX1(10).setX2(20).build();

    proxy.mul(new AnswerCallback(), req);

    server.shutdown();
  }

  public class AnswerCallback implements Callback<MulResponse> {
    @Override
    public void onComplete(MulResponse response) {
      answer1 = response;
      assertEquals(200, answer1.getResult());
    }

    @Override
    public void onFailure(Throwable error) {
    }
  }

}
