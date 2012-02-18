/**
 * 
 */
package nta.engine.ipc;

import nta.engine.Abortable;
import nta.engine.LeafServerProtos.AssignTabletRequestProto;
import nta.engine.LeafServerProtos.ReleaseTabletRequestProto;
import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.QueryUnitProtos.QueryUnitRequestProto;
import nta.engine.Stoppable;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;

/**
 * @author jihoon
 *
 */
public interface AsyncWorkerInterface extends Stoppable, Abortable {

	/**
	 * 질의 구문 및 질의 제어 정보를 LeafServer에게 전달 
	 * 
	 * @param request 질의 구문 및 질의 제어 정보
	 * @return 서브 질의에 대한 응답
	 */
	public SubQueryResponseProto requestSubQuery(SubQueryRequestProto request) throws Exception;
	
	public SubQueryResponseProto requestQueryUnit(QueryUnitRequestProto request) throws Exception;
	
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
   * @param serverName
   * @return ServerStatus (protocol buffer)
   * @throws 
   */
  public ServerStatusProto getServerStatus(NullProto request);
}
