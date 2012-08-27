package tajo.engine.ipc;

import tajo.rpc.Callback;
import tajo.rpc.RemoteException;

public interface QueryClientInterface {

  String executeQuery(String query) throws RemoteException;

  void executeQueryAsync(Callback<String> callback, String query);

  void attachTable(String name, String path) throws RemoteException;

  void detachTable(String name) throws RemoteException;

  boolean existsTable(String name) throws RemoteException;

}
