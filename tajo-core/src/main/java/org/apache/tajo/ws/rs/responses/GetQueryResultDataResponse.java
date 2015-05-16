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

package org.apache.tajo.ws.rs.responses;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.ipc.ClientProtos;

import com.google.gson.annotations.Expose;

public class GetQueryResultDataResponse {

  @Expose private ClientProtos.ResultCode resultCode;
  @Expose private Schema schema;
  @Expose private int bytesNum;
  @Expose private ResultSetInfoResponse resultset;
  @Expose private String errorMessage;
  @Expose private String errorTrace;
  
  public ClientProtos.ResultCode getResultCode() {
    return resultCode;
  }
  public void setResultCode(ClientProtos.ResultCode resultCode) {
    this.resultCode = resultCode;
  }
  public Schema getSchema() {
    return schema;
  }
  public void setSchema(Schema schema) {
    this.schema = schema;
  }
  public int getBytesNum() {
    return bytesNum;
  }
  public void setBytesNum(int bytesNum) {
    this.bytesNum = bytesNum;
  }
  public ResultSetInfoResponse getResultset() {
    return resultset;
  }
  public void setResultset(ResultSetInfoResponse resultset) {
    this.resultset = resultset;
  }
  public String getErrorMessage() {
    return errorMessage;
  }
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }
  public String getErrorTrace() {
    return errorTrace;
  }
  public void setErrorTrace(String errorTrace) {
    this.errorTrace = errorTrace;
  }
  
}
