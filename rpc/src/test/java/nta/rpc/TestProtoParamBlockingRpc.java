package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.rpc.test.DummyProtos.MulRequest1;
import nta.rpc.test.DummyProtos.MulRequest2;
import nta.rpc.test.DummyProtos.MulResponse;
import nta.rpc.test.DummyProtos.InnerRequest;
import nta.rpc.test.DummyProtos.InnerResponse;
import nta.rpc.test.DummyProtos.InnerNode;

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
    public void throwException(MulRequest1 request) throws IOException;
    public MulResponse mul(MulRequest1 req1, MulRequest2 req2);
    public int primitiveTypeTest(String inStr, int value);
    public InnerResponse innerProtocolbufferTest(InnerRequest in);
  }

  public static interface DummyClientInterface {
    public void throwException(MulRequest1 request) throws RemoteException;
    public MulResponse mul(MulRequest1 req1, MulRequest2 req2) throws RemoteException;
    public int primitiveTypeTest(String inStr, int value);
    public InnerResponse innerProtocolbufferTest(InnerRequest in);
  }

  public static class DummyServer implements DummyServerInterface {
    @Override
    public void throwException(MulRequest1 request) throws IOException {
      throw new IOException();
    }
    
    @Override
    public MulResponse mul(MulRequest1 req1, MulRequest2 req2) {
      int x1_1 = req1.getX1();
      int x1_2 = req1.getX2();
      int x2_1 = req2.getX1();
      int x2_2 = req2.getX2();

      int result1 = x1_1 * x2_1;
      int result2 = x1_2 * x2_2;

      MulResponse rst = MulResponse.newBuilder().setResult1(result1).setResult2(result2).build();
      return rst;
    }
    
    @Override
    public int primitiveTypeTest(String inStr, int value) {
      System.out.println("Server" + inStr);
      return value;
    }
    
    @Override
    public InnerResponse innerProtocolbufferTest(InnerRequest in) {
      InnerResponse res = null;
      InnerResponse.Builder builder = InnerResponse.newBuilder();
      for( InnerNode node : in.getNodesList() ) {
        builder.addNodes(node);
      }
      res = builder.build();
      return res;
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
  public void testRpcRemoteException() throws Exception {
    MulRequest1 req = MulRequest1.newBuilder().setX1(10).setX2(20).build();

    try {
      proxy.throwException(req);
    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }

    server.shutdown();
  }

  @Test
  public void testRpcProtoType() throws Exception {

    // 3.2 Fill request data
    MulRequest1 req1 = MulRequest1.newBuilder().setX1(10).setX2(20).build();
    MulRequest2 req2 = MulRequest2.newBuilder().setX1(10).setX2(20).build();

    // 3.3 call procedure
    try {
      MulResponse re = proxy.mul(req1, req2);
      assertEquals(100, re.getResult1());
      assertEquals(400, re.getResult2());
      
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
  public void testInnerProtocolBuffer() throws Exception {
    String strs[] = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
    InnerRequest.Builder builder = InnerRequest.newBuilder();

    try {
      for( String str : strs) {
        InnerNode node = InnerNode.newBuilder().setInstr(str).build();
        builder.addNodes(node);
      }
      InnerRequest req = builder.build();
      InnerResponse resp = proxy.innerProtocolbufferTest(req);
      for( InnerNode node :resp.getNodesList() ) {
        System.out.println(node.getInstr());
      }
    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }
  }
  
}
