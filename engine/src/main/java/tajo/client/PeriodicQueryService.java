package tajo.client;

import tajo.client.PeriodicQueryProtos.*;
import nta.engine.ClientServiceProtos.ExecuteQueryRespose;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;

public interface PeriodicQueryService {
  public StatusResponse registerNewPeriodicQuery(QueryStatusProto request);
  public StatusResponse executePeriodicQuery(ChooseQueryRequest request);
  public StatusResponse regiAndexeNewPeriodicQuery(QueryStatusProto request);
  public StatusResponse cancelPeriodicQuery(ChooseQueryRequest request);
  public NullProto executeAllPeriodicQuery(NullProto request);
  public NullProto cancelAllPeirodicQuery(NullProto request);
  public QueryListResponse getQueryList(NullProto request);
  public ExecuteQueryRespose getQueryResultPath(ChooseQueryRequest request);
  public QueryResultInfoResponse getQueryResultInfo(ChooseQueryRequest request);

}
