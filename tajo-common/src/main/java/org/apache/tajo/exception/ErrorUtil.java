/**
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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tajo.error.Errors;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.error.Stacktrace;

public class ErrorUtil {
  public static boolean isOk(ResultCode code) {
    return code == ResultCode.OK;
  }

  public static boolean isFailed(ResultCode code) {
    return code != ResultCode.OK;
  }

  public static Stacktrace.StackTrace convertStacktrace(Throwable t) {
    Stacktrace.StackTrace.Builder builder = Stacktrace.StackTrace.newBuilder();
    for (StackTraceElement element : t.getStackTrace()) {
      builder.addElement(Stacktrace.StackTrace.Element.newBuilder()
              .setFilename(element.getFileName() == null ? "(Unknown Source)" : element.getFileName())
              .setFunction(element.getClassName() + "::" + element.getMethodName())
              .setLine(element.getLineNumber())
      );
    }
    return builder.build();
  }

  public static Errors.SerializedException convertException(Throwable t) {
    Errors.SerializedException.Builder builder = Errors.SerializedException.newBuilder();

    if (ExceptionUtil.isExceptionWithResultCode(t)) {
      DefaultTajoException tajoException = (DefaultTajoException) t;
      builder.setReturnCode(tajoException.getErrorCode());
      builder.setMessage(tajoException.getMessage());
    } else {
      Throwable rootCause = ExceptionUtils.getRootCause(t);
      if(rootCause != null) t = rootCause;

      builder.setReturnCode(ResultCode.INTERNAL_ERROR);
      builder.setMessage(ErrorMessages.getInternalErrorMessage(t));
    }
    builder.setStackTrace(ErrorUtil.convertStacktrace(t));
    builder.setTimestamp(System.currentTimeMillis());
    return builder.build();
  }
}
