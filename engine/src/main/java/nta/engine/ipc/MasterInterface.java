/**
 * 
 */
package nta.engine.ipc;

import nta.engine.MasterInterfaceProtos.PingRequestProto;
import nta.engine.MasterInterfaceProtos.PingResponseProto;

/**
 * @author jihoon
 *
 */
public interface MasterInterface {

  /**
   * Worker들의 질의 수행 경과를 Master에게 전달
   * 
   * @param report 질의 및 질의 경과 정보
   */
  public PingResponseProto reportQueryUnit(PingRequestProto report);
}
