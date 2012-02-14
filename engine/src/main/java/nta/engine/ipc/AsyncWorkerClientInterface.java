/**
 * 
 */
package nta.engine.ipc;

import nta.engine.LeafServerProtos.AssignTabletRequestProto;
import nta.engine.LeafServerProtos.ReleaseTabletRequestProto;
import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.QueryUnitProtos.QueryUnitRequestProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.rpc.Callback;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;

/**
 * @author jihoon
 *
 */
public interface AsyncWorkerClientInterface {

	/**
	 * 질의 구문 및 질의 제어 정보를 LeafServer에게 전달 
	 * 
	 * @param request 질의 구문 및 질의 제어 정보
	 * @return 서브 질의에 대한 응답
	 */
	public void requestSubQuery(Callback<SubQueryResponseProto> callback, SubQueryRequestProto request) throws Exception;
	
	public void requestQueryUnit(Callback<SubQueryResponseProto> callback, QueryUnitRequestProto request) throws Exception;
	/**
	 * LeafServer에게 테이블 서빙을 할당 함
	 * @param request
	 */
	public void assignTablets(AssignTabletRequestProto request);
	
	/**
	 * LeafServer에게 테이블 서빙 할당을 취소함
	 * @param request
	 */	
	public void releaseTablets(ReleaseTabletRequestProto request);
	
  /**
   * LeafServer의 상태 정보를 가져옴
   * 
   * @param callback
   * @return ServerStatus (protocol buffer)
   * @throws
   */
  public void getServerStatus(Callback<ServerStatusProto> callback, NullProto request);
}
