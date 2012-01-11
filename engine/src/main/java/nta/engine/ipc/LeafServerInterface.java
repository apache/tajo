package nta.engine.ipc;

import org.apache.hadoop.ipc.VersionedProtocol;

import nta.engine.Abortable;
import nta.engine.Stoppable;
import nta.engine.ipc.protocolrecords.AssignTabletRequest;
import nta.engine.ipc.protocolrecords.ReleaseTableRequest;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.SubQueryResponse;

/**
 * LeafServerInterface is the RPC interface that the query engines across cluster nodes provide. 
 * 
 * @author hyunsik
 *
 */
public interface LeafServerInterface extends VersionedProtocol, Stoppable, Abortable {
	
	public final long versionID = 0;
	
	/**
	 * 질의 구문 및 질의 제어 정보를 LeafServer에게 전달 
	 * 
	 * @param request 질의 구문 및 질의 제어 정보
	 * @return 서브 질의에 대한 응답
	 */
	public SubQueryResponse requestSubQuery(SubQueryRequest request) throws Exception;
	
	/**
	 * LeafServer에게 테이블 서빙을 할당 함
	 * @param request
	 */
	public void assignTablets(AssignTabletRequest request);
	
	/**
	 * LeafServer에게 테이블 서빙 할당을 취소함
	 * @param request
	 */	
	public void releaseTablets(ReleaseTableRequest request);
}
