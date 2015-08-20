package org.apache.tajo.exception;

import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

/**
 *
 * This exception occurs when both inputs of a cross join are not the simple relation.
 */
public class InvalidInputsForCrossJoin extends TajoException {

  public InvalidInputsForCrossJoin(ReturnState e) {
    super(e);
  }

  public InvalidInputsForCrossJoin() {
    super(ResultCode.INVALID_INPUTS_FOR_CROSS_JOIN);
  }
}
