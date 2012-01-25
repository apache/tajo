package nta.engine.ipc;

import nta.catalog.TableDescImpl;
import nta.rpc.Callback;
import nta.rpc.RemoteException;

public interface QueryClientInterface {

  String executeQuery(String query) throws RemoteException;

  void executeQueryAsync(Callback<String> callback, String query);

  @Deprecated
  void createTable(TableDescImpl meta);

  @Deprecated
  void dropTable(String name);

  void attachTable(String name, String path) throws RemoteException;

  void detachTable(String name) throws RemoteException;

  boolean existsTable(String name) throws RemoteException;

}
