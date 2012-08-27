package tajo.client;

import tajo.catalog.proto.CatalogProtos.TableDescProto;
import tajo.engine.ClientServiceProtos.*;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;


/**
 * @author Hyunsik Choi
 */
public interface ClientService {
  
  BoolProto existTable(StringProto name) throws RemoteException;
  
  ExecuteQueryRespose executeQuery(ExecuteQueryRequest query) throws RemoteException;
  
  AttachTableResponse attachTable(AttachTableRequest request) throws RemoteException;
  
  void detachTable(StringProto name) throws RemoteException;
  
  CreateTableResponse createTable(CreateTableRequest request) throws RemoteException;
  
  void dropTable(StringProto name) throws RemoteException;
  
  GetClusterInfoResponse getClusterInfo(GetClusterInfoRequest request) throws RemoteException;
  
  GetTableListResponse getTableList(GetTableListRequest request) throws RemoteException;
  
  TableDescProto getTableDesc(StringProto tableName) throws RemoteException;
}
