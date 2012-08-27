/**
 * 
 */
package tajo.engine.ipc;

import tajo.engine.MasterInterfaceProtos.*;
import tajo.rpc.Callback;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;

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
	public void requestQueryUnit(Callback<SubQueryResponseProto> callback, QueryUnitRequestProto request) throws Exception;
	
	public void requestCommand(Callback<CommandResponseProto> callback, CommandRequestProto request);
	
  /**
   * LeafServer의 상태 정보를 가져옴
   * 
   * @param callback
   * @return ServerStatus (protocol buffer)
   * @throws
   */
  public void getServerStatus(Callback<ServerStatusProto> callback, NullProto request);
}
