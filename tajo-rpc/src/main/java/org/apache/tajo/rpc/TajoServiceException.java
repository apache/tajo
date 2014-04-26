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

import com.google.protobuf.ServiceException;
import org.apache.commons.lang.exception.ExceptionUtils;

public class TajoServiceException extends ServiceException {
  private String traceMessage;
  private String protocol;
  private String remoteAddress;

  public TajoServiceException(String message) {
    super(message);
  }
  public TajoServiceException(String message, String traceMessage) {
    super(message);
    this.traceMessage = traceMessage;
  }

  public TajoServiceException(String message, Throwable cause, String protocol, String remoteAddress) {
    super(message, cause);

    this.protocol = protocol;
    this.remoteAddress = remoteAddress;
  }

  public String getTraceMessage() {
    if(traceMessage == null && getCause() != null){
      this.traceMessage = ExceptionUtils.getStackTrace(getCause());
    }
    return traceMessage;
  }

  public String getProtocol() {
    return protocol;
  }

  public String getRemoteAddress() {
    return remoteAddress;
  }
}
