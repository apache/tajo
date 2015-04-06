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

import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.util.TUtil;

import com.google.gson.annotations.Expose;

import java.util.Map;

public class NewSessionResponse {

  @Expose private String id;
  @Expose private String message;
  @Expose private ClientProtos.ResultCode resultCode;
  @Expose private Map<String, String> variables;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ClientProtos.ResultCode getResultCode() {
    return resultCode;
  }

  public void setResultCode(ClientProtos.ResultCode resultCode) {
    this.resultCode = resultCode;
  }

  public Map<String, String> getVariables() {
    if (variables == null) {
      variables = TUtil.newHashMap();
    }
    return variables;
  }

  public void setVariables(Map<String, String> variables) {
    getVariables().putAll(variables);
  }

  @Override
  public String toString() {
    return "NewSessionResponse [id=" + id + ", message=" + message + ", resultCode=" + resultCode + ", variables="
        + variables + "]";
  }
  
}
