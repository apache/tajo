/**
 * 
 */
package tajo.engine.ipc;

import tajo.Abortable;
import tajo.engine.MasterInterfaceProtos.*;
import tajo.Stoppable;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;

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
	public SubQueryResponseProto requestQueryUnit(QueryUnitRequestProto request) throws Exception;
	
	public CommandResponseProto requestCommand(CommandRequestProto request);
	
	 /**
   * LeafServer의 상태 정보를 가져옴
   * @param serverName
   * @return ServerStatus (protocol buffer)
   * @throws 
   */
  public ServerStatusProto getServerStatus(NullProto request);
}
