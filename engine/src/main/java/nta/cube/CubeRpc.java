package nta.cube;

import nta.rpc.Callback;

public class CubeRpc {
  public static interface ServerInterface {
    public void send(int nodenum) throws InterruptedException;
  }

  public static interface ClientInterface {
    public void send(Callback<Integer> callback, int nodenum);
  }

  public static class Server implements ServerInterface {
    @Override
    public void send(int nodenum) throws InterruptedException {
      ServerEngn.getWriteFinNode(nodenum);
    }
  }
}
