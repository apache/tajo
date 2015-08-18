package org.apache.tajo.exception;

import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

public class TooLargeTableForCrossJoinExceptoin extends TajoException {

  public TooLargeTableForCrossJoinExceptoin(ReturnState e) {
    super(e);
  }

}
