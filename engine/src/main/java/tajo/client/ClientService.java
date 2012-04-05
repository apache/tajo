package tajo.client;

import nta.engine.ClientServiceProtos.AttachTableRequest;
import nta.engine.ClientServiceProtos.AttachTableResponse;
import nta.engine.ClientServiceProtos.CreateTableRequest;
import nta.engine.ClientServiceProtos.CreateTableResponse;
import nta.engine.ClientServiceProtos.QuerySubmitRequest;
import nta.engine.ClientServiceProtos.QuerySubmitRespose;
import nta.rpc.RemoteException;
import nta.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;


/**
 * @author Hyunsik Choi
 */
public interface ClientService {
  BoolProto existTable(StringProto name) throws RemoteException;
  QuerySubmitRespose submitQuery(QuerySubmitRequest query) throws RemoteException;
  AttachTableResponse attachTable(AttachTableRequest request) throws RemoteException;
  void detachTable(StringProto name) throws RemoteException;
  CreateTableResponse createTable(CreateTableRequest request) throws RemoteException;
  void dropTable(StringProto name) throws RemoteException;
}
