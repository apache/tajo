package org.apache.tajo.exception;

import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

public class TooLargeResultForCrossJoinException extends TajoException {

  public TooLargeResultForCrossJoinException(ReturnState e) {
    super(e);
  }

  public TooLargeResultForCrossJoinException(long currentThreshold) {
    super(ResultCode.TOO_LARGE_RESULT_FOR_CROSS_JOIN, currentThreshold + "");
  }
}
