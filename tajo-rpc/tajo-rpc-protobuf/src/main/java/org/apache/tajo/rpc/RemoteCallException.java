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

package org.apache.tajo.rpc;

import com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

public class RemoteCallException extends RemoteException {
  private int seqId;
  private String originExceptionClass;

  public RemoteCallException(int seqId, MethodDescriptor methodDesc,
                             Throwable t) {
    super("Remote call error occurs when " + methodDesc.getFullName() + "is called:", t);
    this.seqId = seqId;
    if (t != null) {
      originExceptionClass = t.getClass().getCanonicalName();
    }
  }

  public RemoteCallException(int seqId, Throwable t) {
    super(t);
    this.seqId = seqId;
    if (t != null) {
      originExceptionClass = t.getClass().getCanonicalName();
    }
  }

  public RpcResponse getResponse() {
    RpcResponse.Builder builder = RpcResponse.newBuilder();
    builder.setId(seqId);
    if (getCause().getMessage() == null) {
      builder.setErrorMessage(getCause().getClass().getName());
    } else {
      builder.setErrorMessage(getCause().getMessage());
    }
    builder.setErrorTrace(getStackTraceString(getCause()));
    builder.setErrorClass(originExceptionClass);

    return builder.build();
  }

  private static String getStackTraceString(Throwable aThrowable) {
    final Writer result = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(result);
    aThrowable.printStackTrace(printWriter);
    return result.toString();
  }
}
