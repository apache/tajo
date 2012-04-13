package tajo.client;

import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.ClientServiceProtos.AttachTableRequest;
import nta.engine.ClientServiceProtos.AttachTableResponse;
import nta.engine.ClientServiceProtos.CreateTableRequest;
import nta.engine.ClientServiceProtos.CreateTableResponse;
import nta.engine.ClientServiceProtos.ExecuteQueryRequest;
import nta.engine.ClientServiceProtos.ExecuteQueryRespose;
import nta.engine.ClientServiceProtos.GetClusterInfoRequest;
import nta.engine.ClientServiceProtos.GetClusterInfoResponse;
import nta.engine.ClientServiceProtos.GetTableListRequest;
import nta.engine.ClientServiceProtos.GetTableListResponse;
import nta.rpc.RemoteException;
import nta.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;


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
