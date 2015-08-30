/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.exception;

import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

/**
 * This is an runtime exception container to enclose a TajoException, an actual cause.
 *
 * @see @{link TajoException}
 */
public class TajoRuntimeException extends RuntimeException implements DefaultTajoException {
  private ResultCode code;

  public TajoRuntimeException(ReturnState state) {
    super(ExceptionUtil.toTajoException(state));
    this.code = state.getReturnCode();
  }

  public TajoRuntimeException(ResultCode code, String ... args) {
    super(
        ExceptionUtil.toTajoException(
            ReturnState.newBuilder()
                .setReturnCode(code)
                .setMessage(ErrorMessages.getMessage(code, args))
                .build()
        )
    );
    this.code = code;
  }

  public TajoRuntimeException(TajoException e) {
    super(e);
    this.code = e.getErrorCode();
  }

  @Override
  public ResultCode getErrorCode() {
    return code;
  }
}
