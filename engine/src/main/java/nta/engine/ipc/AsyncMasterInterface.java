/**
 * 
 */
package nta.engine.ipc;

import nta.engine.QueryUnitProtos.QueryUnitReportProto;

/**
 * @author jihoon
 *
 */
public interface AsyncMasterInterface {

  /**
   * Worker들의 질의 수행 경과를 Master에게 전달
   * 
   * @param report 질의 및 질의 경과 정보
   */
  public void reportQueryUnit(QueryUnitReportProto report);
}
