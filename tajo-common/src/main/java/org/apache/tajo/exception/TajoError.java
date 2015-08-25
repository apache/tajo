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
 * Unrecoverable errors
 */
public class TajoError extends Error implements DefaultTajoException {
  private ResultCode code;

  public TajoError(ReturnState state) {
    super(state.getMessage());
    code = state.getReturnCode();
  }

  public TajoError(ResultCode code) {
    super(ErrorMessages.getMessage(code));
    this.code = code;
  }

  public TajoError(ResultCode code, Throwable t) {
    super(ErrorMessages.getMessage(code), t);
    this.code = code;
  }

  public TajoError(ResultCode code, String ... args) {
    super(ErrorMessages.getMessage(code, args));
    this.code = code;
  }

  public TajoError(ResultCode code, Throwable t, String ... args) {
    super(ErrorMessages.getMessage(code, args), t);
    this.code = code;
  }

  @Override
  public ResultCode getErrorCode() {
    return code;
  }
}
