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

package org.apache.tajo.client;

import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.ErrorUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.ipc.ClientProtos;

/**
 *
 */
public class ExceptionConvertor {

  private static boolean isManagedException(Throwable t) {
    return t instanceof TajoException || t instanceof TajoRuntimeException;
  }

  public ClientProtos.ReturnState convert(Throwable t) {

    ClientProtos.ReturnState.Builder builder = ClientProtos.ReturnState.newBuilder();

    if (isManagedException(t)) {


    } else {
      // uncaught Exception, RuntimeException, and Error are mostly bugs,
      // and they should be handled as internal error.

      builder.setReturnCode(Errors.ResultCode.INTERNAL_ERROR);

      if (t.getMessage() != null) {
        builder.setMessage(t.getMessage());
      }

      builder.setStackTrace(ErrorUtil.convertStacktrace(t));
    }

    return builder.build();
  }

}
